from pydantic import BaseModel


class ManualTriggerRequest(BaseModel):
    submission_id: str
    tenant_code: str
    submission_type: str
