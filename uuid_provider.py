import uuid


class UUIDProvider:
    NAMESPACE_HAL = uuid.uuid3(uuid.NAMESPACE_DNS, 'hal.science')
    NAMESPACE_ADUM = uuid.uuid3(uuid.NAMESPACE_DNS, 'adum.fr')

    def __init__(self, ns: uuid.UUID, val: str) -> None:
        self.val = val
        self.ns = ns

    def value(self) -> uuid.UUID:
        return uuid.uuid3(self.ns, self.val)
