# Formulio Addon
Hello, this is the standalone Formula Racing addon for Stremio!

This can be run standalone, please run formula-addon.py.
That's it!

Please make sure you have all the required requirements.txt

## You can build & run the docker image locally with
```
docker build -t formulio-addon .
```
```
docker run -p 8000:8000 formulio-addon
```

The Dockerfile will create a user called "appuser" and this will be used to run the container as non-root

## How this funky script works
formulio-addon.py will run the several 1formationlap.py scripts, that will execute the following scripts in it's respective directory.
1formationlap.py
2dbsorter.py
3racedirector.py
4retire.py
5torrenttocontent.py
6merger.py

It'll then watch each 6processed.txt file for modification, and if there is a change, it'll quickly restart the webserver, to host new content.

Feel free to propose changes
