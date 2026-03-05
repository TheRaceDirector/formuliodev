# Formulio Addon
Hello, this is the standalone Formula Racing addon for Stremio!

This can be run standalone, please run formula-addon.py.
That's it!

Please make sure you have all the required requirements.txt

## You can build & run the docker image locally
First ensure you're in the main directory (the one with all the files in)
```
docker build -t formulio-addon .
```
```
docker run -p 8000:8000 formulio-addon
```

The Dockerfile will create a user called "appuser" and this will be used to run the container as non-root

Feel free to propose changes
