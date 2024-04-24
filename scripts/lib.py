from pyquadkey2.quadkey import QuadKey


def get_quadkeys(level):
    qk = QuadKey("0")
    return qk.children(at_level=level)
