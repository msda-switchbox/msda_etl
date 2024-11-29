"""A module for generating random values, dates, etc"""

# pylint: disable=invalid-name


def static_vars(**kwargs):
    """Static variable decorator"""

    def decorate(func):
        for k, v in kwargs.items():
            setattr(func, k, v)
        return func

    return decorate


@static_vars(count=0)
def generate_int_primary_key() -> int:
    """Generate a primary key"""
    # pylint: disable=E1101
    generate_int_primary_key.count += 1
    # pylint: disable=E1101
    return generate_int_primary_key.count
