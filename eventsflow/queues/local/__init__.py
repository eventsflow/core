
from . import context


globals().update((name, getattr(context._default_context, name))
                 for name in context._default_context.__all__)
__all__ = context._default_context.__all__

