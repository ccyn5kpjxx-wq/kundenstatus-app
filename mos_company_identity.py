"""Registered company details for new individual MOS booking documents.

Historical signed documents keep their stored, immutable bytes. Update these
details only against a current official register extract.
"""

LEGAL_NAME = 'Gärtner GmbH Karosserie + Lack'
BUSINESS_ADDRESS = 'Binauer Höhe 4, 74821 Mosbach, Deutschland'
REGISTERED_SEAT = 'Mosbach'
REGISTER_COURT = 'Amtsgericht Mannheim'
REGISTER_NUMBER = 'HRB 754425'
MANAGING_DIRECTOR = 'Christopher Gärtner'


def business_letter_details():
    return ('Sitz: ' + REGISTERED_SEAT + '\n'
            'Registergericht: ' + REGISTER_COURT + '\n'
            'Handelsregister: ' + REGISTER_NUMBER + '\n'
            'Geschäftsführer: ' + MANAGING_DIRECTOR)
