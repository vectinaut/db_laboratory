from mimesis import Person
from mimesis.locales import Locale
from mimesis.enums import Gender
from mimesis import Address
from mimesis import Text
from mimesis import Internet
from mimesis import Datetime

from faker import Faker

person = Person(Locale.RU)
address = Address(Locale.RU)
text = Text(Locale.RU)
internet = Internet()
dt = Datetime(Locale.RU)

fake = Faker()


def get_name(type_name, gender):

    gender_dict = {0: Gender.MALE, 1: Gender.FEMALE}

    if type_name == 'first_name':
        first_name = person.first_name(gender=gender_dict[gender])
        return first_name

    elif type_name == 'last_name':
        last_name = person.last_name(gender=gender_dict[gender])
        return last_name


def get_date(type_date):
    if type_date == 'birth_date':
        return str(dt.date(start=1940, end=2000))
    else:
        return str(dt.date(start=2018))


def get_telephone():
    return person.telephone()


def get_password():
    return person.password(hashed=True)


def get_city():
    return address.city()


def get_email():
    return person.email()


def get_text(type_text):
    # return text.word()
    if type_text == 'title':
        return text.title()
    elif type_text == 'description':
        return text.text(quantity=3)
    elif type_text == 'comment':
        return text.sentence()


def get_url():
    return internet.url() + internet.slug()


def get_education():
    return person.university()
