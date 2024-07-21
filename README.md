## RootsMatchIt v0.1 - Build Your Family Tree with DNA Matches!

**RootsMatchIt** helps you import your DNA match data (DNAGedcom) RootsMagic 10, creating a family tree for each match and automatically linking them based on DNA connections. 

**What you'll need:**

* Your DNAGedcom database file (obtained from the DNAGedcom client).
* An empty RootsMagic 10 database

**Getting Started:**

1. **Prepare your RootsMagic Database:**
    * Open RootsMagic 10 and create a new, empty database.
    * Close RootsMagic 10 after creating the database. **Important:** This allows RootsMatchIt to properly access the file.    * 

2. **Place your files:**
    * Copy your DNAGedcom file into the `./db` directory within the RootsMatchIt folder.
    *  Place the empty RootsMagic 10 database file (**.RMTREE**) in the same `./db` directory.

3. **Rebuild Indices:**
    * In RootsMagic 10, go to Tools > Rebuild Index.

4. **Import your existing family tree (Optional):**
    * In RootsMagic 10, import your existing family tree data (if you have one). 
    * **Tip:**  For best results, only include your direct ancestors and descendants during this import.  You can then merge your ancestors' siblings from the DNA match data imported by RootsMatchIt. RootsMagic handles this merging process more efficiently than the script could.

5. **Run RootsMatchIt:** 
Run the file and select the Ancestry Profiles you want to include. Don't include any Profiles together that are not related. Select the Gender for each profile selected, and run the script.

**What to expect:**

* RootsMatchIt will process your Ancestry profiles from your DNAGedcom file.
    * If a match has a linked family tree, RootsMatchIt will create a new tree in your RootsMagic database for that match and link them based on DNA connections.
    * If a match doesn't have a linked tree, RootsMatchIt will still create an entry in your RootsMagic database but only include their DNA information.
* Future versions will include support for importing family trees from other DNA testing services like FTDNA and MyHeritage.

**Important Notes:**

* RootsMatchIt currently does not handle duplicate entries.  This is because RootsMagic offers better functionality for merging duplicates within the program itself.
* Make sure to back up your existing RootsMagic database before using RootsMatchIt.