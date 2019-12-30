from cx_Freeze import setup, Executable

if __name__ == '__main__':
    setup(
        name="RamanBroker",
        version="0.1",
        executables=[Executable("main.py")]
    )
