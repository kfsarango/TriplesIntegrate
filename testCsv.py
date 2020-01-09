import csv

f = open('dt/names.csv', 'w')

with f:
    fnames = ['first_name', 'last_name']
    writer = csv.DictWriter(f, fieldnames=fnames)

    writer.writeheader()
    writer.writerow({'first_name': 'John', 'last_name': 'Smith'})
    writer.writerow({'first_name': 'Robert', 'last_name': 'Brown'})
    writer.writerow({'first_name': 'Julia', 'last_name': 'Griffin'})