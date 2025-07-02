class GeocodeError(Exception):
    """Exception raised when a geocoding operation fails."""

    user_message: str

    def __init__(self, message: str) -> None:
        self.user_message = message
        super().__init__(self.user_message)
