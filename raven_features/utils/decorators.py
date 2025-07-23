import difflib


def validate_allowed_args(*allowed_args):
    """
    Decorator that validates keyword arguments and suggests close matches for invalid ones.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            for arg in kwargs:
                if arg not in allowed_args:
                    suggestion = difflib.get_close_matches(arg, allowed_args, n=1)
                    if suggestion:
                        raise ValueError(
                            f"Argument '{arg}' is not allowed. Did you mean '{suggestion[0]}'?"
                        )
                    else:
                        raise ValueError(
                            f"Argument '{arg}' is not allowed. Allowed arguments: {allowed_args}"
                        )
            return func(*args, **kwargs)
        return wrapper
    return decorator



def validate_mutex_args(*mutex_args):
    """
    Decorator to validate that only one of the specified keyword arguments is provided.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Extract the keyword arguments that are not None and are in mutex_args
            non_none_kwargs = {k: v for k, v in kwargs.items() if v is not None and k in mutex_args}
            # Check if more than one keyword argument is provided
            if len(non_none_kwargs) > 1:
                raise ValueError("Only one of the following arguments can be provided: {}".format(", ".join(non_none_kwargs.keys())))
            return func(*args, **kwargs)
        return wrapper
    return decorator

def validate_mutex_groups(*groups):
    """
    Decorator to validate that arguments from only one group are truthy.
    Each group is a list of parameter names. If truthy arguments from multiple groups are used, raise an error.
    """
    # Map each param to its group index
    param_to_group = {}
    for i, group in enumerate(groups):
        for param in group:
            param_to_group[param] = i

    def decorator(func):
        def wrapper(*args, **kwargs):
            active_groups = set()
            for param, value in kwargs.items():
                if value and param in param_to_group:
                    active_groups.add(param_to_group[param])
            if len(active_groups) > 1:
                raise ValueError("Parameters from multiple exclusive groups were provided.")
            return func(*args, **kwargs)
        return wrapper
    return decorator


def require_at_least_one(*required_keys):
    def decorator(func):
        def wrapper(*args, **kwargs):
            if not any(bool(kwargs.get(key)) for key in required_keys):
                received = {k: kwargs.get(k) for k in required_keys}
                raise ValueError(
                    f"At least one of the following arguments is required (and must be truthy): {', '.join(required_keys)}. Got: {received}"
                )
            return func(*args, **kwargs)
        return wrapper
    return decorator




def validate_required_args(*required_keys):
    """
    Decorator to ensure that required keyword arguments are provided.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            missing = [key for key in required_keys if key not in kwargs]
            if missing:
                raise ValueError(f"Missing required keyword arguments: {', '.join(missing)}")
            return func(*args, **kwargs)
        return wrapper
    return decorator