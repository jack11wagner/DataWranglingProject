# DataWranglingProject - Baseball Statistics

**Project Description:**
     
- WebScraping data from Retrosheet.com using Beautiful Soup

- Performing data cleaning and adding new columns to create a database with scraped data

- Categories:

     - All-Time Career Leaders
     - Top Individual Performances
     - Awards & Honors
     - Individual Career Batting/Pitching Stats

---

- Data Cleaning Cases:

    - Some dates in Top Individual Performance tables had unneccessary whitespace in the string, which was cleaned up
    - Some dates also had numbers in parentheses and asterisks at the end of the string that were removed
    - Some data from all time stats had random 'i' characters amongst data
    - In Hall of Fame Records, found instances of non players in the hall of fame
    - Found issues with matching up player names, some tables used nicknames while others used full names to identify players
    - Invalid Dates Found in Player Bios Txt File replaced with 0000-01-01 in database

---

- Individual Career Stats:
     
     - Using the various playerIDs we were able to search up player URLs and do webscrapes for their individual career stat lines
     - Made sure to limit webscraping to 7 players per minute to make sure to not abuse retrosheets web server
     - Wrote player data to batting/pitching csv files


--- 


- New Columns:

    - Column to find length between debut game and last game in playerbios
    - *** Possibly a column for Win/Loss Ratio ***


- Copyright Message:

     + The information used here was obtained free of
     charge from and is copyrighted by Retrosheet.  Interested
     parties may contact Retrosheet at "www.retrosheet.org".
