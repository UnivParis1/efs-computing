def choose_author_identifier(auth):
    idhal_s = auth.get('idhal_s', None)
    if idhal_s and len(str(idhal_s).strip()) > 0:
        return f"s-{idhal_s}"
    idhal_i = int(auth.get('idhal_i', 0))
    if idhal_i:
        return f"i-{str(idhal_i)}"
    assert len(str(auth.get('form_id')).strip()) > 0
    return f"f-{auth.get('form_id')}"
