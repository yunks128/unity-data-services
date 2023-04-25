class GranuleMetadataProps:
    def __init__(self):
        self.__beginning_dt = None
        self.__ending_dt = None
        self.__collection_name = None
        self.__collection_version = None
        self.__granule_id = None
        self.__prod_dt = None
        self.__insert_dt = None

    @property
    def beginning_dt(self):
        return self.__beginning_dt

    @beginning_dt.setter
    def beginning_dt(self, val):
        """
        :param val:
        :return: None
        """
        self.__beginning_dt = val
        return

    @property
    def ending_dt(self):
        return self.__ending_dt

    @ending_dt.setter
    def ending_dt(self, val):
        """
        :param val:
        :return: None
        """
        self.__ending_dt = val
        return

    @property
    def collection_name(self):
        return self.__collection_name

    @collection_name.setter
    def collection_name(self, val):
        """
        :param val:
        :return: None
        """
        self.__collection_name = val
        return

    @property
    def collection_version(self):
        return self.__collection_version

    @collection_version.setter
    def collection_version(self, val):
        """
        :param val:
        :return: None
        """
        self.__collection_version = val
        return

    @property
    def granule_id(self):
        return self.__granule_id

    @granule_id.setter
    def granule_id(self, val):
        """
        :param val:
        :return: None
        """
        self.__granule_id = val
        return

    @property
    def prod_dt(self):
        return self.__prod_dt

    @prod_dt.setter
    def prod_dt(self, val):
        """
        :param val:
        :return: None
        """
        self.__prod_dt = val
        return

    @property
    def insert_dt(self):
        return self.__insert_dt

    @insert_dt.setter
    def insert_dt(self, val):
        """
        :param val:
        :return: None
        """
        self.__insert_dt = val
        return
