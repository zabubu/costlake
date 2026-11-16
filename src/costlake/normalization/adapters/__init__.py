from costlake.config import SourceFormat
from costlake.normalization.adapters.aws_cur2 import AwsCur2Adapter
from costlake.normalization.adapters.base import Adapter, AdapterResult, ContractViolationError
from costlake.normalization.adapters.focus import FocusAdapter

__all__ = ["Adapter", "AdapterResult", "ContractViolationError", "adapter_for"]

_ADAPTERS: dict[SourceFormat, Adapter] = {
    SourceFormat.FOCUS: FocusAdapter(),
    SourceFormat.AWS_CUR2: AwsCur2Adapter(),
}


def adapter_for(fmt: SourceFormat) -> Adapter:
    return _ADAPTERS[fmt]
