# Formulio Addon
Hello, this is the standalone Formula Racing addon for Stremio!

This can be run standalone, please run formula-addon.py.
That's it!

Please make sure you have all the required requirements.txt
Also in static/index.html, change "127.0.0.1" to be your IP or domain of choice.

Very open to changes

## You can build the docker image with
```
docker build -t formulio-addon .
```
```
docker run -p 8000:8000 formulio-addon
```

The Dockerfile will create a user called "appuser" and this will be used to run the container as non-root

## How this funky script works
formulio-addon.py will run the several 0run_scripts.py scripts, that 0run_scripts.py file will execute the following scripts in it's respective directory.
1rsstocsv.py
2dbsorter.py
3racedirector.py
4torrenttocontent.py
5merger.py

It'll then watch each 5processed.txt file for modification, and if there is a change, it'll quickly restart the webserver, to host new content. 