import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from cosync_direct_restore import DirectReceiver
from cosync_publish_batch import discover_slots, run_once, run_titles
from test_codex_contract import read_rpc_response, stop_server
import nexus_sync as ns


@unittest.skipUnless(os.environ.get('NEXUS_TEST_CODEX') == '1' and shutil.which('codex'), 'requires disposable Codex')
class DiscoveryCodexTests(unittest.TestCase):
    def test_new_chat_with_child_is_published_and_readable_on_receiver(self):
        with tempfile.TemporaryDirectory(prefix='cosyncx-discovery-real-') as tmp:
            root=Path(tmp).resolve()
            sender,exchange,titles,graph,receiver,backup,project,state=(root/x for x in
                ('sender','exchange','titles','graph','receiver','backup','project','state'))
            for p in (sender,exchange,titles,graph,receiver,backup,project,state):p.mkdir(mode=0o700)
            (sender/'sessions').mkdir();(exchange/'sessions').mkdir()
            parent='019b0000-0000-7000-8000-000000000001';child='019b0000-0000-7000-8000-000000000002'
            def path(identity):return sender/'sessions'/f'rollout-2026-01-01T00-00-00-{identity}.jsonl'
            def meta(identity,source,**extra):return {'timestamp':'2026-01-01T00:00:00.000Z','type':'session_meta',
                'payload':{'id':identity,'timestamp':'2026-01-01T00:00:00.000Z','cwd':'/source/Nexus',
                'originator':'codex_vscode','cli_version':'0.1.0','source':source,'model_provider':'openai',**extra}}
            root_rows=[meta(parent,'vscode'),{'timestamp':'2026-01-01T00:00:01.000Z','type':'response_item',
                'payload':{'type':'message','role':'user','content':[{'type':'input_text','text':'new conversation'}]}},
                {'timestamp':'2026-01-01T00:00:01.001Z','type':'event_msg','payload':{'type':'user_message',
                'message':'new conversation','images':[],'local_images':[],'text_elements':[]}}]
            child_rows=[meta(child,{'subagent':{'other':'guardian'}},parent_thread_id=parent),
                {'timestamp':'2026-01-01T00:00:02.000Z','type':'event_msg','payload':{'type':'agent_message','message':'child result'}}]
            path(parent).write_text(''.join(json.dumps(x)+'\n' for x in root_rows))
            path(child).write_text(''.join(json.dumps(x)+'\n' for x in child_rows))
            codex=Path(shutil.which('codex'))
            result=subprocess.run([str(codex),'migrate-rollouts','--apply','--json'],
                env={**os.environ,'CODEX_HOME':str(sender)},capture_output=True,timeout=30)
            self.assertEqual(result.returncode,0)
            with sqlite3.connect(sender/'state_5.sqlite') as db:
                db.execute('INSERT INTO thread_spawn_edges (parent_thread_id,child_thread_id,status) VALUES (?,?,?)',
                           (parent,child,'closed'))
            (sender/'session_index.jsonl').write_text(json.dumps({'id':parent,'thread_name':'New chat'})+'\n')
            (titles/'session_index.jsonl').write_text('')
            (graph/'graph.json').write_text(json.dumps({'format':'cosyncx-active-graph-v1','edges':[],
                'omitted_missing':0,'omitted_archived':0}))
            config={'profile':str(sender),'exchange':str(exchange),'discovery_state':str(state),'slots':[],
                'title_source':str(sender/'session_index.jsonl'),'title_destination':str(titles/'session_index.jsonl'),
                'title_lock':str(root/'title.lock'),'local_root':'/source/Nexus','remote_root':str(project),
                'discovery_graph':str(graph),'discovery_device':'a'*32}
            slots,counts=discover_slots(config,ns._chat_catalog(sender,include_unsupported=True),{})
            self.assertEqual(counts,{'DISCOVERED_CHILD':1,'DISCOVERED':1})
            extended={**config,'slots':slots};cache={}
            self.assertEqual(run_once(extended,cache),{'UNCHANGED':2})
            self.assertEqual(run_titles(extended,cache),{'PUBLISHED':1})
            (receiver/'config.toml').write_text('[features]\nplugins = false\nremote_plugin = false\n')
            server=subprocess.Popen([str(codex),'app-server','--listen','stdio://'],stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,bufsize=0,env={**os.environ,'CODEX_HOME':str(receiver)})
            try:
                server.stdin.write((json.dumps({'id':1,'method':'initialize','params':{
                    'clientInfo':{'name':'cosyncx-test','version':'0.1'},'capabilities':{'experimentalApi':True}}})+'\n').encode());server.stdin.flush()
                self.assertIn('result',read_rpc_response(server,1))
                server.stdin.write(b'{"method":"initialized","params":{}}\n');server.stdin.flush()
                server.stdin.write(b'{"id":2,"method":"thread/list","params":{"limit":1}}\n');server.stdin.flush()
                self.assertIn('result',read_rpc_response(server,2))
            finally:stop_server(server)
            consumer=DirectReceiver(source=exchange,titles=titles,graph=graph,profile=receiver,backup_root=backup,
                key=b'k'*32,mappings=[('/source/Nexus',str(project))],codex=codex,
                folder_ids=('sessions','titles','graph'),peer_id='vps')
            with patch('cosync_direct_restore.quiescence.is_quiet',return_value=True):
                self.assertEqual(consumer._receive(),2)
            with sqlite3.connect(receiver/'state_5.sqlite') as db:
                self.assertEqual(db.execute('SELECT parent_thread_id,status FROM thread_spawn_edges WHERE child_thread_id=?',
                                            (child,)).fetchone(),(parent,'closed'))
                self.assertEqual(db.execute('SELECT count(*) FROM threads WHERE id IN (?,?) AND archived=0',
                                            (parent,child)).fetchone()[0],2)
            self.assertEqual({parent,child}&set(ns._chat_catalog(receiver,include_unsupported=True)),{parent,child})


if __name__=='__main__':unittest.main()
