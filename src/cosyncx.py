"""CoSyncX command-line entry point."""
import sys

import cosync_engine
import cosync_vendor
import cosync_proxy
import nexus_sync
import cosync_extensions
import cosync_lineage
import cosync_receive_plan
import cosync_graph_snapshot


def main(argv=None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments and arguments[0] == 'engine':
        return cosync_engine.main(arguments[1:])
    if arguments and arguments[0] == 'ui':
        return cosync_proxy.main(arguments[1:])
    if arguments and arguments[0] == 'build-engine':
        return cosync_vendor.main(arguments[1:])
    if arguments and arguments[0] == 'extensions':
        return cosync_extensions.main(arguments[1:])
    if arguments and arguments[0] == 'prepare-lineage':
        return cosync_lineage.main(arguments[1:])
    if arguments and arguments[0] == 'receive-plan':
        return cosync_receive_plan.main(arguments[1:])
    if arguments and arguments[0] == 'graph-snapshot':
        return cosync_graph_snapshot.main(arguments[1:])
    return nexus_sync.main(arguments)


if __name__ == '__main__':
    raise SystemExit(main())
