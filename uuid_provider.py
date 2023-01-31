import uuid


class UUIDProvider:
    NAMESPACE_P1 = uuid.uuid3(uuid.NAMESPACE_DNS, 'univ-paris1.fr')

    def __init__(self, val: str) -> None:
        self.val = val

    def value(self) -> uuid.UUID:
        return uuid.uuid3(self.NAMESPACE_P1, self.val)
