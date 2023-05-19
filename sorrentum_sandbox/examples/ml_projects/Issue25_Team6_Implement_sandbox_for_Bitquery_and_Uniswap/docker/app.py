from flask import Flask, render_template, request

app = Flask(__name__, template_folder="templates")


@app.route("/")
def index():
    return render_template("webform.html")


@app.route("/submit", methods=["POST"])
def submit():
    hist_date = request.form["hist_date"]
    result = hist_date
    return f"The date chosen is {result}!"


# def process_name(name):
#     # Import python function here for inserting and running the api
#     return name.upper()

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8000)
