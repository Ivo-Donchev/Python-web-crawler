import requests
import urllib.request
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


engine = create_engine('sqlite:///websites.db/')
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()


def add_and_commit(obj):
    session.add(obj)
    session.commit()


class Website(Base):
    __tablename__ = 'websites'
    website_id = Column(Integer, primary_key=True)
    website_url = Column(String(50), index=True)
    website_server = Column(String(50))
    website_checked = Column(Boolean, unique=False, default=False)

    def __str__(self):
        return "url: {}-------server: {}".format(self.website_url,
                                                 self.website_server)

    def __repr__(self):
        return str(self)


Base.metadata.create_all(engine)

servers = []


def make_soup(url):
    start_bg = urllib.request.urlopen(url)
    html = start_bg.read()
    soup = BeautifulSoup(html, 'html.parser')
    return soup


def print_servers(session):
    print("Servers:")
    for s in session.query(Website):
        print(s)


def get_domain(url):
    parsed_uri = urlparse(url)
    domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
    return domain


def add_to_database(url, server):
    ev = session.query(Website).filter(url == url)
    if not ev:
        ev = session.Website(url=url, website_server=server)
        session.add(ev)
    else:
        print('found in database')


def exists(session, model, **kwargs):
    try:
        obj = session.query(model).filter_by(**kwargs).one()
        return obj
    except NoResultFound:
        return None


def _get_or_create(session, model, defaults=None, **kwargs):
    obj = exists(session, model, **kwargs)

    if obj is not None:
        return obj

    obj = model(**kwargs)
    session.add(obj)
    session.commit()

    return obj


def get_direct_link(link):
    try:
        link_location = link.headers['Location']
        link_location = get_domain(link_location)
        if link_location.endswith('bg/'):
            print(link_location)
            original_link_head = requests.head(link_location)
            server = original_link_head.headers['Server']
            _get_or_create(session,
                           Website,
                           website_url=link_location,
                           website_server=server)
        else:
            print('Bad link: {}'.format(link_location))

    except (requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout) as e:
        print(e)


def get_link_full(link):
    if re.match(r'^http', link):
        try:
            link = get_domain(link)
            if link.endswith('bg/'):
                print(link)
                original_link_head = requests.head(link)
                server = original_link_head.headers['Server']
                _get_or_create(session,
                               Website,
                               website_url=link,
                               website_server=server)
            else:
                print('Bad link: {}'.format(link))
        except (requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            print(e)


def get_all_links(url):
    for a in make_soup(url).find_all('a', href=True):
        link = requests.head(url + a['href'])
        if('Location' in link.headers):
            get_direct_link(link)
        else:
            get_link_full(a['href'])
        session.commit()


def add_to_servers(servers, all_websites_data):
    for element in all_websites_data:
        server = element.website_server
        is_there = False
        for idx in range(len(servers)):
            if(servers[idx][0] == server):
                tmp = list(servers[idx])
                tmp[1] += 1
                servers[idx] = tuple(tmp)
                is_there = True
        if is_there is False:
            servers.append((server, 1))


def search(url):
    get_all_links(url)
    session.commit()


def start(session):
    current_url = session.query(Website).\
        filter(Website.website_checked.is_(False)).first()
    while(current_url is not None):
        search(current_url.website_url)
        current_url.website_checked = True
        session.commit(current_url)
        current_url = session.query(Website).\
            filter(Website.website_checked is False).first()

# url = 'http://register.start.bg/'
url = 'http://abv.bg/'

_get_or_create(session,
               Website,
               website_url=url,
               website_server=requests.get(url).headers['Server'])
session.commit()
start(session)
# add_to_servers(servers, session.query(Website).all())
# print_servers(session)
