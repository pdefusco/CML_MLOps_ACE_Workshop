import dash
import dash_core_components as dcc
import dash_html_components as html
import flask

server = flask.Flask(__name__, static_url_path='')
app = dash.Dash(__name__, server=server)

#app = dash.Dash()

app.layout = html.Div("Dash Test")

if __name__ == '__main__':
    #app.run_server(debug=True)
    app.run_server(host='127.0.0.1', port=int(os.environ['CDSW_READONLY_PORT']))