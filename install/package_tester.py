#!/usr/bin/env python


def _get_version(lib_name):
    version = None
    try:
        cmd = "import %s" % lib_name
        exec(cmd)
    except ImportError:
        version = "- (can't import)"
    else:
        cmd = "%s.__version__" % lib_name
        version = eval(cmd)
    return version


def get_system_signature():
    txt = []
    # import sys
    # print(sys.version)
    import platform

    txt.append(("python", platform.python_version()))
    libs = [
        "numpy",
        "pandas",
        "pyarrow",
        "joblib",
        "scipy",
        "seaborn",
        "sklearn",
        "statsmodels",
    ]
    libs = sorted(libs)
    # TODO(gp): Add date, git commit.
    for lib in libs:
        txt.append((lib, _get_version(lib)))
    txt_out = ["%15s: %s" % (l, v) for (l, v) in txt]
    return "\n".join(txt_out)


if __name__ == "__main__":
    print(get_system_signature())
