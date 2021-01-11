class GenericFunctions:

    @classmethod
    def folder_to_table(cls, name):
        return name.replace("__", ".")

    @classmethod
    def table_to_folder(cls, name):
        return name.replace(".", "__")
