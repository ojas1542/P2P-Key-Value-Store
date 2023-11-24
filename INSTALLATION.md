# getting set up for development
1. install python 3.11
1. in the root of the repository, create a new venv. (`python` in the below command should be
   whatever your python 3.11 executable is - if you're on windows it's likely `py -3.11`)
   ```
   python -m venv .venv
   ```
1. activate the venv (if you're using vscode with the python extension you can just open a new
   terminal and it should activate it for you)
1. install dev dependencies
   ```
   pip install -r requirements_dev.txt
   ```

# running server
to run the server (outside of docker):
```
python -m kvs
```
or, alternatively
```
python -m flask --app kvs.app run
```
for development, you may also want to run it with live reloading on, you can do that via:
```
python -m flask --app kvs.app --debug run
```

# type checking
## command line
to type check the entire project, run:
```
mypy -p kvs
```

## vscode
the standard vscode python plugin supports mypy, but it isn't on by default. to enable it, just add
the following to your workspace `settings.json`:
```json
{
    "python.linting.enabled": true,
    "python.linting.mypyEnabled": true,
    "python.linting.lintOnSave": true,
    // (we don't want to also get type checks from the language server, just from mypy)
    "python.analysis.typeCheckingMode": "off",
}
```

# tests
to run tests:
1. create kv_subnet if you haven't already
   ```
   docker network create --subnet=10.10.0.0/16 kv_subnet
   ```
1. rebuild image if necessary:
   ```
   docker build -t kvs:3.0 .
   ```
1. to run all tests:
   ```
   python -m unittest discover
   ```
   to run specific tests (replace `test_name_here` with test name):
   ```
   python -m unittest tests.TestAssignment3.test_name_here
   ```

