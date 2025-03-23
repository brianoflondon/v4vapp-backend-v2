from typing import Any, Dict

from pydantic import BaseModel

from v4vapp_backend_v2.helpers.general_purpose_funcs import snake_case


class OpBase(BaseModel):

    @classmethod
    def name(cls) -> str:
        return snake_case(cls.__name__)

    @property
    def log_extra(self) -> Dict[str, Any]:
        return {self.name(): self.model_dump()}
