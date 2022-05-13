# http://docs.opengeospatial.org/per/20-025r1.html#_get_collectionsobservationitems         (multiple items)

# http://docs.opengeospatial.org/per/20-025r1.html#_get_collectionsobservationitemsfeatureid   (single item)

# http://docs.opengeospatial.org/per/20-025r1.html#_get_collectionsobservation          (all items in a collection)

from flask import Flask
from .v1 import blueprint


def get_app():
    app = Flask(__name__)
    app.register_blueprint(blueprint)
    # api.init_app(app)
    # CORS(app)
    return app
