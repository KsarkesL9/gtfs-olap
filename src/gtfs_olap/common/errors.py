"""Własne klasy wyjątków używane w ETL.

Dzięki nim możemy w entry-poincie łapać konkretne typy błędów i wyświetlać
użytkownikowi pomocne komunikaty zamiast surowego stack-trace.
"""


class ETLError(Exception):
    """Bazowy wyjątek ETL - wszystkie nasze błędy dziedziczą po tym."""


class CkanError(ETLError):
    """Problem z CKAN API: brak odpowiedzi, nieprawidłowy format itp."""


class NoMatchingPackage(ETLError):
    """Nie znaleziono żadnej paczki pokrywającej żądany okres."""


class GtfsParseError(ETLError):
    """Plik GTFS jest niepoprawny: brak wymaganych kolumn, błędny format."""


class DatabaseError(ETLError):
    """Problem z bazą: brak połączenia, brak uprawnień, niezgodny schemat."""
