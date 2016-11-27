# to run from terminal
import sys

# network related
import requests
from bs4 import BeautifulSoup

# orm related
from sqlalchemy import (
    create_engine,
    update,
    Column,
    Integer,
    String,
    Boolean
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class Domain(Base):
    __tablename__ = 'urls'

    id = Column(Integer, primary_key=True)
    domain = Column(String, nullable=True)
    server = Column(String, nullable=True)
    is_scraped = Column(Boolean, default=False)

    def __repr__(self):
        return "<Url(domain='%s', server='%s')>" % (
               self.domain, self.server)


class Database:
    def __init__(self, name):
        self.name = name
        self.engine = create_engine('sqlite:///{}'.format(self.name), echo=True)
        Base.metadata.create_all(self.engine)

class Crawler:
    def __init__(self, url, database):
        self.current_url = url
        self.database = database
        self.found_links = []
        self.session_maker = sessionmaker(bind=self.database.engine)
        self.session = self.session_maker()
        if self._get_start_url() is not None:
            self.current_url = self._get_start_url()

    def search_for_url(self, url):
        """
        Scrapes current url page, gets all bg domains and saves it to database
        """
        bg_domains = self.get_all_bg_domains(url=url)
        print(bg_domains)
        self.save_links_to_database(links=bg_domains)

    def search(self):
        """
        To start searching if DB is empty.
        TODO: Refactor This
        """
        if not self._exists_in_database(self.current_url):
            server = self.get_server(self.current_url)
            obj = Domain(domain=self.current_url, server=server)
            self.session.add(obj)
            self.session.commit()

        while self._get_start_url():
            self.search_for_url(url=self._get_start_url())

    def _get_start_url(self):
        qs = self.session.query(Domain.domain)\
                         .filter(Domain.is_scraped == False)
        if qs.count() == 0:
            return None
        return qs.first()[0]

    def _get_raw_html(self, url=None):
        if url is None:
            url = self.current_url

        '''
        If server returns internal error or current_url is invalid
        or inactive we don't want our crawler to stop.
        Current solution is to just return '' and go on.
        '''
        try:
            response = requests.get(self.current_url, timeout=3)
            return response.text
        except Exception as err:
            print("Oops: " + str(err))
            return ''

    def get_server(self, url=None):
        if url is None:
            url = self.current_url
        try:
            response = requests.get(self.current_url, timeout=3)
            return response.headers['server']
        except Exception as err:
            print("Oops: " + str(err))
            return ''

    def make_soup(self):
        raw_html = self._get_raw_html()
        return BeautifulSoup(raw_html)

    def _get_all_links(self):
        return self.make_soup().find_all('a')

    def _get_all_hrefs_from_links(self, url=None):
        if url is None:
            url = self.current_url
        hrefs = []
        for link in self._get_all_links():
            hrefs.append(link.get('href', ''))
        return hrefs

    def get_all_links_to_external_sites(self, url=None):
        if url is None:
            url = self.current_url

        all_hrefs = self._get_all_hrefs_from_links(url)
        return [link for link in all_hrefs if link.startswith('http')]

    def get_all_links_to_current_site(self, url=None):
        if url is None:
            url = self.current_url

        all_hrefs = self._get_all_hrefs_from_links(url)
        return [link for link in all_hrefs if link.startswith('/')]

    def get_all_bg_domains(self, url=None):
        if url is None:
            url = self.current_url

        domains = set()
        bg_sufix = '.bg/'
        for link in self.get_all_links_to_external_sites():
            if bg_sufix in link:
                idx = link.find(bg_sufix)
                domains.add(link[:(idx + len(bg_sufix))])

        return list(domains)

    def _exists_in_database(self, url):
        qs = self.session.query(Domain.domain)\
                         .filter(Domain.domain == url)
        return qs.count() != 0

    def save_links_to_database(self, links=[]):
        for link in links:
            if not self._exists_in_database(link):
                server = self.get_server(link)
                obj = Domain(domain=link, server=server)
                self.session.add(obj)
                self.session.commit()

        update_start_url = update(Domain).where(Domain.domain==self._get_start_url())\
                                         .values(is_scraped=True)
        self.session.execute(update_start_url)
        self.session.commit()


def main(argv):
    my_db = Database(argv[0])
    current_url = argv[1]
    my_crawler = Crawler(url=current_url, database=my_db)
    my_crawler.search()

if __name__ == "__main__":
   main(sys.argv[1:])