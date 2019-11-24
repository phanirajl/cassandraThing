import json
from flask import abort
import uuid
from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect('library')


class BookKeeping:
    def __init__(self, card_id, book_id, date):
        self.card_id = card_id
        self.book_id = book_id
        self.date = date

    def __serialized(self):
        return {
            'card_id': self.card_id,
            'book_id': self.book_id,
            'date': str(self.date),
        }

    def to_json(self):
        return json.dumps(self.__serialized())


class BookKeepingManager:
    def __init__(self):
        session.execute(
            "CREATE TABLE IF NOT EXISTS library.book_keeping (card_id text, book_id text, date date, PRIMARY KEY(book_id, card_id));"
        )

    def create(self, card_id, book_id, date):
        res = session.execute(
            "INSERT INTO library.book_keeping (card_id, book_id, date, status) VALUES ('{}', '{}', '{}') IF NOT EXISTS;".format(
                card_id, book_id, date)
        )
        if not res.one().applied:
            abort(500)
        return BookKeeping(card_id, book_id, date).to_json()

    def find_by_book_id(self, book_id):
        res = session.execute(
            "SELECT * FROM library.books_keeping WHERE book_id = '{}';".format(
                book_id)
        )
        response_array = []
        for book_keep in res:
            response_array.append(
                BookKeeping(book_keep.card_id, book_keep.book_id, book_keep.date).to_json())
        return json.dumps(response_array)


class BookManager:
    def __init__(self):
        session.execute(
            "CREATE TABLE IF NOT EXISTS library.books_by_author (author_id text, book_id text, title text, genre text, year_published text, PRIMARY KEY(author_id, title));")
        session.execute(
            "CREATE TABLE IF NOT EXISTS library.books_by_title (author_id text, book_id text, title text, genre text, year_published text, PRIMARY KEY(title, year_published));")

    def create(self, author_id, title, genre, year_published):
        uid = uuid.uuid4().hex
        try:
            session.execute(
                "INSERT INTO library.books_by_author (author_id, book_id, title, genre, year_published) VALUES ('{}', '{}', '{}', '{}', '{}');".format(author_id, uid, title, genre, year_published))
            session.execute(
                "INSERT INTO library.books_by_title (author_id, book_id, title, genre, year_published) VALUES ('{}', '{}', '{}', '{}', '{}');".format(author_id, uid, title, genre, year_published))
            return Book(author_id, uid, title, genre, year_published).to_json()
        except:
            abort(500)

    def find_by_author(self, author_id):
        res = session.execute(
            "SELECT * FROM library.books_by_author WHERE author_id = '{}';".format(
                author_id)
        )
        response_array = []
        for book in res:
            response_array.append(
                Book(book.author_id, book.book_id, book.title, book.genre, book.year_published).to_json())
        return json.dumps(response_array)

    def find_by_title(self, title):
        res = session.execute(
            "SELECT * FROM library.books_by_title WHERE title = '{}';".format(
                title)
        )
        response_array = []
        for book in res:
            response_array.append(
                Book(book.author_id, book.book_id, book.title, book.genre, book.year_published).to_json())
        return json.dumps(response_array)


class Book:
    def __init__(self, author_id, book_id, title, genre, publish_year):
        self.author_id = author_id
        self.book_id = book_id
        self.title = title
        self.genre = genre
        self.year_published = publish_year

    def __serialized(self):
        return {
            'author_id': self.author_id,
            'book_id': self.book_id,
            'title': self.title,
            'genre': self.genre,
            'publish_year': self.year_published
        }

    def to_json(self):
        return json.dumps(self.__serialized())


class Author:
    def __init__(self, author_id, full_name, year_born):
        self.author_id = author_id
        self.full_name = full_name
        self.year_born = year_born

    def __serialized(self):
        return {
            'author_id': self.author_id,
            'full_name': self.full_name,
            'year_born': str(self.year_born),
        }

    def to_json(self):
        return json.dumps(self.__serialized())


class AuthorManager:
    def __init__(self):
        session.execute(
            "CREATE TABLE IF NOT EXISTS library.authors (author_id text, full_name text, year_born date, PRIMARY KEY(full_name, year_born, author_id));")

    def create(self, full_name, year_born):
        uid = uuid.uuid4().hex
        try:
            session.execute(
                "INSERT INTO library.authors (author_id, full_name, year_born) VALUES ('{}', '{}', '{}');".format(uid, full_name, year_born))
            return Author(uid, full_name, year_born).to_json()
        except:
            abort(500)

    def find_by_full_name(self, full_name):
        res = session.execute(
            "SELECT * FROM library.authors WHERE full_name = '{}';".format(
                full_name)
        )
        response_array = []
        for author in res:
            response_array.append(
                Author(author.author_id, author.full_name, author.year_born).to_json())
        return json.dumps(response_array)

    def find_by_full_name_and_date(self, full_name, year_born):
        res = session.execute(
            "SELECT * FROM library.authors WHERE full_name = '{}' and year_born = '{}';".format(
                full_name, year_born)
        )
        response_array = []
        for author in res:
            response_array.append(
                Author(author.author_id, author.full_name, author.year_born).to_json())
        return json.dumps(response_array)
