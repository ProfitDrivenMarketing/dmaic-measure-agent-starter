# Make "app.schemas" importable as an alias of "app.models.schemas"
import sys
from .models import schemas as _schemas_module
sys.modules[__name__ + ".schemas"] = _schemas_module

