class AppError(Exception):
    """Base application error"""
    pass


class AirflowError(AppError):
    pass


class BackendAPIError(AppError):
    pass


class DataError(AppError):
    pass
