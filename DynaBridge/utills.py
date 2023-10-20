from datetime import datetime


class TimeUtills:
    @classmethod
    def get_current_utc_datetime(cls):
        """
        The function `get_current_utc_datetime` returns the current UTC datetime in the format "YYYY-MM-DD
        HH:MM:SS".
        
        :param cls: The `cls` parameter in this context refers to the class itself. It is used in a class
        method to access class-level variables or methods. In this case, it is not used within the method,
        so it can be removed
        :return: The method `get_current_utc_datetime` returns a formatted string representing the current
        UTC datetime in the format "YYYY-MM-DD HH:MM:SS".
        """
        try:
            formatted_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            return formatted_time
        except Exception as e:
            raise e
