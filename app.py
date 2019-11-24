from flask import Flask, request, jsonify
import json
from models import AuthorManager, BookManager, BookKeepingManager

APP = Flask(__name__)

author_manager = AuthorManager()
book_manager = BookManager()
book_keeping_manager = BookKeepingManager()


@APP.route('/book_keeping', methods=['POST'])
def register_book():
    return book_keeping_manager.create(
        request.json['card_id'],
        request.json['book_id'],
        request.json['date'],
    )


@APP.route('/book_keeping/<book_id>', methods=['GET'])
def get_registered_book(book_id):
    return book_keeping_manager.find_by_book_id(book_id)


@APP.route('/books', methods=['POST'])
def create_book():
    return book_manager.create(
        request.json['author_id'],
        request.json['title'],
        request.json['genre'],
        request.json['year_published']
    )


@APP.route('/authors', methods=['POST'])
def create_user():
    return author_manager.create(
        request.json['full_name'],
        request.json['year_born']
    )


@APP.route('/books/author/<author_id>', methods=['GET'])
def get_book_by_author(author_id):
    return book_manager.find_by_author(author_id)


@APP.route('/books/title/<title>', methods=['GET'])
def get_book_by_title(title):
    return book_manager.find_by_title(title)


@APP.route('/authors/<full_name>', methods=['GET'])
def get_author_by_fullname(full_name):
    return author_manager.find_by_full_name(full_name)


@APP.route('/authors/<full_name>/<year_born>', methods=['GET'])
def get_author_by_fullname_and_date(full_name, year_born):
    return author_manager.find_by_full_name_and_date(full_name, year_born)
