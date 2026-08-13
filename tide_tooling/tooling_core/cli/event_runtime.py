from tide_tooling.tooling_core.engine.events import Event, RouterEvents, OceanEvents, PipelineEvents
from tide_tooling.tooling_core.engine.utils import tide_snapshot_value

def start_event_cli(event_interface, store_router) -> None: # Move to ocean
    import threading
    def cli_loop():
        while True:
            cmd = input("event> ")
            _parse_cli_cmd_into_event(cmd, event_interface, store_router)
    t = threading.Thread(target=cli_loop, daemon=True)
    t.start()

def _parse_cli_cmd_into_event(cmd: str, event_interface, store_router) -> Event:
    import shlex
    shlex_tokens = shlex.split(cmd)
    command = shlex_tokens[0] if len(shlex_tokens) > 0 else None
    kv_pairs = shlex_tokens[1:] if command else None

    ocean_event_consumer = event_interface.emit_event_to_ocean

    try:
        if command is None:
            return None

        if command == "kill":
            if not kv_pairs:
                print("No runner_id given to kill!")
                return
            runner_id = int(kv_pairs[0])
            ocean_event_consumer(Event(source="ocean", source_id=0, target_runner_id=runner_id, event_type=OceanEvents.KILL_PIPELINE))
            return
        # if command == "print":
        #     ocean_event_consumer(Event(source="ocean", source_id=0, target_runner_id='*', event_type=PipelineEvents.PRINT))

        # if command == "register":
        #     eval_flag=False
        #     if "=" not in kv_pairs[0]:
        #         try:
        #             func = _safe_parse_func_from_string(kv_pairs[0])
        #             eval_flag = True
        #             kv_pairs = kv_pairs[1:]
        #         except Exception as e:
        #             print(e)
        #             pass
            
        #     for pair in kv_pairs:
        #         if "=" not in pair:
        #             raise ValueError(f"[RegisterNodeCmdError] {pair} is not a valid command input. Must contain '='. e.g. register func=func args=... kwargs=... target=@target")
                
        #         key, value = pair.split("=",1)
        #         parsed[key] = value
                

        #     print("registered:", parsed)
            
        #     if not eval_flag:
        #         if "func" not in parsed:
        #             raise ValueError("The 'register' command is missing: func! Please provide at least a func=... arg")
        #         func = parsed.pop("func")
        #         func = _safe_parse_func_from_string(func)
        #     target = parsed.get("target")
        #     if target:
        #         target = parsed.pop("target")
            
        #     for k, v in parsed.items():
        #         if _is_valid_node_input(k):
        #             parsed[k] = _ast_parse_value_from_string(v)
        #         else:
        #             raise TypeError(f"Error registering node! {k} is not a valid node input!")
        #     print(parsed)
            
        #     args = parsed.get("args")
        #     if not args:
        #         args = ()
        #     elif not isinstance(args, (list, tuple)):
        #         args = (args, )

        #     node = Node(func=func, args=args, kwargs=parsed.get("kwargs"), p_args=parsed.get("p_args"), p_kwargs=parsed.get("p_kwargs"), k_args=parsed.get("k_args"), k_kwargs = parsed.get("k_kwargs"), target=target)

        #     target_pipeline = parsed.get("pipeline")
        #     if not target_pipeline:
        #         target_pipeline = 0

        #     print(f"emitting register_node event to pipeline {target_pipeline}")
        #     ocean_event_consumer(Event(source="ocean", source_id=0, target_runner_id=target_pipeline, event_type=PipelineEvents.REGISTER_NODE, payload=node))

        if command == "run_pipeline" or command == "run":
            if kv_pairs:
                mode = 'auto'
                pipeline_id = int(kv_pairs[0])
                if len(kv_pairs) > 1:
                    mode = kv_pairs[1]
                ocean_event_consumer(Event(source="ocean", source_id=0, target_runner_id=None, event_type=OceanEvents.RUN_PIPELINE, payload=(pipeline_id, mode, {}, True)))
                return
            print("No pipeline to run! Please register one! and specify with 0 or filepath")
            return
            
        
        if command == "delete":
            key = None
            store = 'mem'
            runner_id = '*'

            if not kv_pairs:
                print("Nothing to delete!")
                return
            if kv_pairs[0]:
                key = kv_pairs[0]
            if len(kv_pairs) > 1:
                store = kv_pairs[1]
            if len(kv_pairs) > 2:
                runner_id = int(kv_pairs[2])

            if not key:
                print(f"No key to delete - was {key}. Returning.")
                return 
            if not store:
                print("No store given - selecting scoped mem_0 as default!")

            # change to emit store_delete event
            ocean_event_consumer(Event(source="ocean", source_id=0, target_runner_id=runner_id, event_type=RouterEvents.DELETE, payload=(store, key, "user", "cmd")))

            
            return

        # if command == "waiting":
        #     0 = kv_pairs[0] if kv_pairs else '*'
        #     ocean_event_consumer(Event(source="ocean", source_id=0, target_runner_id=id, event_type=PipelineEvents.WAITING, payload=None))

        if command == "set":
            key = kv_pairs[0] if len(kv_pairs) > 0 else None
            value = kv_pairs[1] if len(kv_pairs) > 1 else None
            store = kv_pairs[2] if len(kv_pairs) > 2 else 'mem'
            runner_id = int(kv_pairs[3]) if len(kv_pairs) > 3 else '*'

            if not key:
                print("No key to set!")
                return
            if value is None:
                print("No value to set!")
                return
            
            if runner_id == "*":
                if store in store_router.store_alias:
                    for real_name in store_router.store_alias[store]:
                        ocean_event_consumer(Event(source="ocean", source_id=0, target_runner_id=runner_id, event_type=RouterEvents.STORE_WRITE, payload=(real_name, key, value, "user", "cmd", tide_snapshot_value(value, boundary="write"))))
                else:
                    ocean_event_consumer(Event(source="ocean", source_id=0, target_runner_id=runner_id, event_type=RouterEvents.STORE_WRITE, payload=(store, key, value, "user", "cmd", tide_snapshot_value(value, boundary="write"))))
            else:
                store = f"store_{runner_id}"
                ocean_event_consumer(Event(source="ocean", source_id=0, target_runner_id=runner_id, event_type=RouterEvents.STORE_WRITE, payload=(store, key, value, "user", "cmd", tide_snapshot_value(value, boundary="write"))))
            return

        if command == "update":
            key = kv_pairs[0] if len(kv_pairs) > 0 else None
            value = kv_pairs[1] if len(kv_pairs) > 1 else None
            store = kv_pairs[2] if len(kv_pairs) > 2 else 'mem'
            runner_id = int(kv_pairs[3]) if len(kv_pairs) > 3 else '*'
            if not key:
                print("No key to update!")
                return
            if value is None:
                print("No value to update!")
                return
            
            if runner_id == "*":
                if store in store_router.store_alias:
                    for real_name in store_router.store_alias[store]:
                        ocean_event_consumer(Event(source="ocean", source_id=0, target_runner_id=runner_id, event_type=RouterEvents.STORE_WRITE, payload=(real_name, key, value, "user", "cmd", tide_snapshot_value(value, boundary="write"))))
                else:
                    ocean_event_consumer(Event(source="ocean", source_id=0, target_runner_id=runner_id, event_type=RouterEvents.STORE_WRITE, payload=(store, key, value, "user", "cmd", tide_snapshot_value(value, boundary="write"))))
            else:
                store = f"store_{runner_id}"
                ocean_event_consumer(Event(source="ocean", source_id=0, target_runner_id=runner_id, event_type=RouterEvents.STORE_WRITE, payload=(store, key, value, "user", "cmd", tide_snapshot_value(value, boundary="write"))))
            return

        if command == "build":
            key = kv_pairs[0] if len(kv_pairs) > 0 else None
            store = kv_pairs[1] if len(kv_pairs) > 1 else 'mem'
            runner_id = int(kv_pairs[2]) if len(kv_pairs) > 2 else None

            if not key:
                print("No key! nothing to build!")
                return
            if runner_id is None: 
                print("No runner selected! Please choose a runner to build from")
                return

            if store in store_router.store_alias:
                store = f"{store}_{runner_id}"
                ocean_event_consumer(Event(source="ocean", source_id=0, target_runner_id=runner_id, event_type=PipelineEvents.BUILD, payload=(store, key)))
            else:
                print(f"Building {key} from {store}")
                ocean_event_consumer(Event(source="ocean", source_id=0, target_runner_id=runner_id, event_type=PipelineEvents.BUILD, payload=(store, key)))

            return
            # return Event(source="ocean", source_id=0, target_runner_id='*', event_type="build", payload=(store, key))

        # if command == "run_node":
        #     if not kv_pairs[0]:
        #         print("Missing node_id to run!")       
        #         return 
            
        #     node_id = int(kv_pairs[0])
        #     ppl_id = kv_pairs[1] if (len(kv_pairs) > 1) else '*' 

        #     if ppl_id == '*':
        #         print("No pipeline specified! Trying all!")
        #     return Event(source="ocean", source_id=0, target_runner_id='*', event_type="run_node", payload=node_id)    

        # if command == "done":
        #     return Event(source="ocean", source_id=0, target_runner_id='*', event_type="done", payload=None)

        # if command == "save_log":
        #     log_tools.create_log(event_logger.log)
        #     log_tools.save_to_file("ocean_event_log.txt", log_tools.log)
        #     log_tools.save_to_file("execution_logs.txt", log_tools.exec_log.exec_log)
        #     return

        # if command == "quit":
        #     print("Emitting quit event")
        #     return Event(source="ocean", source_id=0, target_runner_id=None, event_type=OceanEvents.QUIT, payload=None)
        # # let event parser handle invalid
        return Event(source="ocean", source_id=0, target_runner_id=None, event_type=command, payload=None)
    except Exception as error:
        print(f"CLI error: {error}")
        return None
