import db
from models import stock


def run():
    """Run: insert data into database"""
    gorra = stock("GOOG", "2021-11-04", 100, 200, 50, 150)
    db.session.add(gorra)
    db.session.commit()


# print(gorra.symbol)


if __name__ == "__main__":

    db.Base.metadata.create_all(db.engine)
    run()
