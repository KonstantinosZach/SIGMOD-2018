## SIGMOD Contest of 2018
* [Contest Description](https://db.in.tum.de/sigmod18contest/task.shtml)
* [Full Report](https://github.com/KonstantinosZach/SIGMOD-2018/blob/main/Report.md)

### Προσωπικά στοιχεία ομάδας:
* Γεώργιος-Κωνσταντίνος Ζαχαρόπουλος
* [Βασιλείου Ρηγίνος](https://github.com/rigas2k19)
* [Γεωργία Σαράφογλου](https://github.com/JoeSarafoglou)
    
### Μεταγλώττιση Εργασίας
Μπορούμε να μεταγλωττίσουμε την εργασία με τους ακόλοθους τρόπους:
* `make` -> μεταγλωττίζει όλα τα προγράμματα και τα tests.
* `make programs` -> μεταγλωττίζει όλα τα προγράμματα.
* `make programs-<program_folder>` -> μεταγλωττίζει τoν targeted program folder.
* `make tests` -> μεταγλωττίζει όλα τα tests.

### Εκτέλεση Εργασίας
* Για να τρέξουμε το τρίτο μέρος της εργασίας θα πρέπει πρώτα να εκτελέσουμε `make programs-main` και μετά `make harness` για να τρέξει το script του harness. Το harness θα τρέξει με το small workload.

* **Σημείωση**: Για να αλλάξουμε το workload σε public θα πρέπει στα αρχεία runTestharness.sh και init.h να αλλάξει το path.

### Χρήση του Valgrind
* Για να τρέξετε valgrind θα πρέπει να τροποποιήσετε το script run.sh. Συγκεκριμένα να αντικαταστήσετε την εντολή `${DIR}/../programs/main/prog` με `valgrind ${DIR}/../programs/main/prog`.

### Καθαρισμός εκτελέσιμων
* `make clean` -> καθαρίζει όλα τα εκτελέσιμα αρχεία.
* `make clean-programs-<τoν targeted program folder>` -> καθαρίζει τα εκτελέσιμα του targeted program folder.

### Σημείωση
* Πρέπει να δώσουμε δικαιώματα (`chmod +x`) στα 2 scripts (run.sh και runTestharness.sh).
* Όταν τρέχουμε το public workload θα πρέπει να αφαιρούμε το Query 12 καθώς γεμίζει η μνήμη και γίνεται killed η εφαρμογή.
