# Flask Application

This is a basic Flask application. Flask is a micro web framework written in Python.

## Prerequisites

Before you begin, ensure you have met the following requirements:
- You have installed the latest version of Python and pip.

## Installing Flask

To install Flask, follow these steps:

```bash
pip install flask
```

## Running the Flask Application

To run the Flask application, follow these steps:

1. Navigate to the directory containing the Flask application file (let's say `app.py`).

```bash
cd /path/to/your/flask/app
```

2. Set the environment variable `FLASK_APP` to be your Flask application file.

```bash
export FLASK_APP=app.py
```

3. Run the Flask application.

```bash
flask run
```

After running the command, you should see output similar to the following:

```bash
* Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)
```

4. Open your web browser and enter the URL provided in the terminal (`http://127.0.0.1:5000/` in this case). You should now be able to see your Flask application running.