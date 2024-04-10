from Advisor.contextBO import ContextBO
from Advisor.tpe import TPEAdvisor
from openbox import logger

def build_optimizer(args, **kwargs):
    task_str = kwargs.get('task_str', 'run')
    optimizer = None
    if args.opt == "contextBO_tsd":
        optimizer = ContextBO(
            config_space=kwargs['config_space'], eval_func=kwargs['eval_func'], iter_num=args.iter_num, surrogate_type='gp',
            ini_context=kwargs['ini_context'],
            method_id=args.opt, task_id=args.task, target=kwargs['target'],
            task_str = task_str,
            tsd_flag='detect', context_flag=True, warm_start_flag=args.warm_start_flag, safe_flag=args.safe_flag, backup_flag=args.backup_flag
        )
    elif args.opt == "contextBO":
        optimizer = ContextBO(
            config_space=kwargs['config_space'], eval_func=kwargs['eval_func'], iter_num=args.iter_num, surrogate_type='gp',
            ini_context=kwargs['ini_context'],
            method_id=args.opt, task_id=args.task, target=kwargs['target'],
            task_str = task_str,
            tsd_flag='no_detect', context_flag=True, warm_start_flag=args.warm_start_flag, safe_flag=args.safe_flag, backup_flag=args.backup_flag
        )
    elif args.opt == 'GP':
        optimizer = ContextBO(
            config_space=kwargs['config_space'], eval_func=kwargs['eval_func'], iter_num=args.iter_num, surrogate_type='gp',
            ini_context=kwargs['ini_context'],
            method_id=args.opt, task_id=args.task, target=kwargs['target'],
            task_str = task_str,
            tsd_flag='no_detect', context_flag=False, warm_start_flag=args.warm_start_flag, safe_flag=args.safe_flag, backup_flag=args.backup_flag
        )

    elif args.opt == 'TPE':
        optimizer = TPEAdvisor(
            config_space=kwargs['config_space'], eval_func=kwargs['eval_func'], iter_num=args.iter_num,
            method_id=args.opt, task_id=args.task, target=kwargs['target'],
            warm_start_flag=args.warm_start_flag, backup_flag=args.backup_flag
        )

    logger.info("[opt: {}] [warm_start_flag: {}] [safe_flag: {}] [backup_flag: {}] [tasks: {}]".format(
        args.opt, args.warm_start_flag, args.safe_flag, args.backup_flag, args.task_swift)
    )
    return optimizer
