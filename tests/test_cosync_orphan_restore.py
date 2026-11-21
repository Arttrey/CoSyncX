import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from cosync_direct_restore import DirectReceiver
import cosync_orphan_restore as orphan
import nexus_sync as ns
from test_codex_contract import read_rpc_response, stop_server


@unittest.skipUnless(os.environ.get('NEXUS_TEST_CODEX') == '1' and shutil.which('codex'), 'requires disposable Codex')
class OrphanRestoreTests(unittest.TestCase):
    def test_interrupted_repair_resumes_and_reindexes_subagent(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-orphan-test-') as tmp:
            root = Path(tmp).resolve()
            source, titles, graph, profile, backups, project = (root / n for n in
                ('source', 'titles', 'graph', 'profile', 'backups', 'project'))
            for p in (source, titles, graph, profile, backups, project): p.mkdir(mode=0o700)
            (profile / 'sessions').mkdir(); (source / 'sessions').mkdir()
            identity = '019b0000-0000-7000-8000-0000000000aa'
            parent = '019b0000-0000-7000-8000-0000000000bb'
            name = f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
            original, received = profile / 'sessions' / name, source / 'sessions' / name
            rows = [
                {'timestamp':'2026-01-01T00:00:00.000Z','type':'session_meta','payload':{
                    'id':identity,'timestamp':'2026-01-01T00:00:00.000Z','cwd':str(project),
                    'originator':'codex_vscode','cli_version':'0.1','source':{'subagent':{'other':'guardian'}},
                    'parent_thread_id':parent,'model_provider':'openai'}},
                {'timestamp':'2026-01-01T00:00:01.000Z','type':'event_msg','payload':{
                    'type':'agent_message','message':'child result'}}]
            original.write_text(''.join(json.dumps(r)+'\n' for r in rows))
            other = profile/'sessions'/f'rollout-2026-01-01T00-00-00-{parent}.jsonl'
            other_rows = [
                {'timestamp':'2026-01-01T00:00:00.000Z','type':'session_meta','payload':{
                    'id':parent,'timestamp':'2026-01-01T00:00:00.000Z','cwd':str(project),
                    'originator':'codex_vscode','cli_version':'0.1','source':'vscode','model_provider':'openai'}},
                {'timestamp':'2026-01-01T00:00:01.000Z','type':'response_item','payload':{
                    'type':'message','role':'user','content':[{'type':'input_text','text':'other chat'}]}},
                {'timestamp':'2026-01-01T00:00:01.001Z','type':'event_msg','payload':{
                    'type':'user_message','message':'other chat','images':[],'local_images':[],'text_elements':[]}}]
            other.write_text(''.join(json.dumps(r)+'\n' for r in other_rows))
            codex = Path(shutil.which('codex'))
            result = subprocess.run([str(codex),'migrate-rollouts','--apply','--json'],
                env={**os.environ,'CODEX_HOME':str(profile)}, capture_output=True, timeout=30)
            self.assertEqual(result.returncode, 0)
            content = original.read_text().splitlines()
            meta = json.loads(content[0]); meta['payload']['cwd'] = '/source/Nexus'
            received.write_text(json.dumps(meta)+'\n'+'\n'.join(content[1:])+'\n')
            original.unlink()
            old = '/old/linux/missing.jsonl'
            with sqlite3.connect(profile/'state_5.sqlite') as db:
                db.execute('UPDATE threads SET rollout_path=?, cwd=? WHERE id=?',(old,'/source/Nexus',identity))
            receiver = DirectReceiver(source=source,titles=titles,graph=graph,profile=profile,backup_root=backups,
                key=b'k'*32,mappings=[('/source/Nexus',str(project))],codex=codex,
                folder_ids=('sessions','titles','graph'),peer_id='vps',allow_active=True)
            entries=ns._chat_catalog(source,include_unsupported=True)[identity]
            server=subprocess.Popen([str(codex),'app-server','--listen','stdio://'],stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,bufsize=0,
                env={**os.environ,'CODEX_HOME':str(profile)})
            try:
                server.stdin.write((json.dumps({'id':1,'method':'initialize','params':{
                    'clientInfo':{'name':'cosyncx-test','version':'0.1'},'capabilities':{'experimentalApi':True}}})+'\n').encode());server.stdin.flush()
                self.assertIn('result',read_rpc_response(server,1))
                server.stdin.write(b'{"method":"initialized","params":{}}\n');server.stdin.flush()
                server.stdin.write((json.dumps({'id':2,'method':'thread/resume','params':{
                    'threadId':parent,'path':str(other),'cwd':str(project)}})+'\n').encode());server.stdin.flush()
                self.assertIn('result',read_rpc_response(server,2))
                before=other.read_bytes()
                with patch('cosync_orphan_restore.indexer._validate_projection_schema',side_effect=ns.SyncError('unknown schema')):
                    with self.assertRaises(ns.SyncError): orphan.prepare(receiver,identity,entries,old,'/source/Nexus')
                self.assertFalse(original.exists())
                self.assertFalse((backups/'direct-orphan-pending.json').exists())
                stale=backups/'direct-orphan-stage.jsonl';stale.write_text('abandoned staging file');stale.chmod(0o600)
                self.assertTrue(orphan.prepare(receiver,identity,entries,old,'/source/Nexus'))
                with sqlite3.connect(profile/'state_5.sqlite') as db:
                    db.execute('UPDATE threads SET rollout_path=? WHERE id=?',('/other/changed.jsonl',identity))
                self.assertFalse(orphan.resume(receiver))
                self.assertFalse(original.exists())
                with sqlite3.connect(profile/'state_5.sqlite') as db:
                    db.execute('UPDATE threads SET rollout_path=? WHERE id=?',(old,identity))
                real_connect=sqlite3.connect
                def fail_writable_db(path,*args,**kwargs):
                    if isinstance(path,Path) and path.name=='state_5.sqlite':
                        raise sqlite3.OperationalError('interrupt before database update')
                    return real_connect(path,*args,**kwargs)
                with patch('cosync_orphan_restore.sqlite3.connect',side_effect=fail_writable_db):
                    with self.assertRaises(sqlite3.OperationalError): orphan.resume(receiver)
                self.assertTrue(original.is_file())
                with sqlite3.connect(profile/'state_5.sqlite') as db:
                    self.assertEqual(db.execute('SELECT rollout_path FROM threads WHERE id=?',(identity,)).fetchone()[0],old)
                with patch('cosync_orphan_restore.indexer._reset_profile_projection',side_effect=ns.SyncError('interrupt')):
                    with self.assertRaises(ns.SyncError): orphan.resume(receiver)
                self.assertTrue((backups/'direct-orphan-pending.json').is_file())
                with sqlite3.connect(profile/'state_5.sqlite') as db:
                    self.assertEqual(db.execute('SELECT rollout_path FROM threads WHERE id=?',(identity,)).fetchone()[0],str(original))
                # The receiver itself resumes the interrupted repair before
                # proceeding to a later batch, even with another chat open.
                save_indexed=orphan._save_indexed
                def save_then_interrupt(*args):
                    save_indexed(*args)
                    raise ns.SyncError('interrupt after verified indexing')
                with patch('cosync_orphan_restore._save_indexed',side_effect=save_then_interrupt):
                    receiver._receive()
                self.assertEqual(json.loads((backups/'direct-orphan-pending.json').read_text())['phase'],'indexed')
                receiver._receive()
                self.assertFalse((backups/'direct-orphan-pending.json').exists())
                self.assertTrue(original.is_file()); self.assertEqual(other.read_bytes(),before)
                self.assertEqual(len(receiver._read_carry()),1)
                server.stdin.write((json.dumps({'id':3,'method':'thread/read','params':{'threadId':parent,'includeTurns':False}})+'\n').encode());server.stdin.flush()
                self.assertIn('result',read_rpc_response(server,3))
            finally:
                stop_server(server)
            # Start from the same stale-path state again and let the normal
            # receiver discover and repair it without calling the helper.
            receiver._write_carry([])
            original.unlink()
            with sqlite3.connect(profile/'state_5.sqlite') as db:
                db.execute('UPDATE threads SET rollout_path=?,cwd=? WHERE id=?',(old,'/source/Nexus',identity))
            shutil.copyfile(other,source/'sessions'/other.name)
            (titles/'session_index.jsonl').write_text(json.dumps({'id':parent,'thread_name':'parent'})+'\n')
            (graph/'graph.json').write_text(json.dumps({'format':'cosyncx-active-graph-v1','edges':[],
                'omitted_missing':0,'omitted_archived':0}))
            receiver._receive()
            self.assertTrue(original.is_file())
            with sqlite3.connect(profile/'state_5.sqlite') as db:
                self.assertEqual(db.execute('SELECT rollout_path FROM threads WHERE id=?',(identity,)).fetchone()[0],str(original))
            self.assertEqual(len(receiver._read_carry()),1)


if __name__ == '__main__': unittest.main()
