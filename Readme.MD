# mirf - Missing Record Finder for SQLite Databases

Mirf is a tool to help discover gaps in rows of SQLite Database tables where columns have the AUTOINCREMENT flag set. 

mirf can help detect if any rows of data were deleted by discovering gaps

mirf uses terminal colors and will need to have `colorama` installed. You can install all requirements by doing:

`pip install -r requirements.txt`

Then run mirf from the terminal:

[![asciicast](https://asciinema.org/a/380843.png)](https://asciinema.org/a/380843)

