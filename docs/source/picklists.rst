Pick List Format
================

Echo-software Columns
---------------------

Source Plate Name, Destination Plate Name
    The names of the source and destination plates, respectively.

Source Plate Barcode, Destination Plate Barcode
    The barcodes of the source and destination plates, respectively.

Source Plate Type, Destination Plate Type
    The plate types of the source and destination plate.  Contrary to Echo documentation, source plate
    types cannot be used for destination plates.

Source Well, Destination Well
    The well names of the source and destination wells, respectively, in standard non-padded format (e.g. A1, B2, C12, etc.).

Source Row, Source Column, Destination Row, Destination Column
    The zero-indexed row and column numbers of the source and destination wells, respectively
    (e.g. A1 is row 0, column 0; B2 is row 1, column 1; C12 is row 2, column 11, etc.). (FIXME: check this)

Sample Name
    A name associated with the Source Well.

Sample Comment, Sample ID, Sample Group
    Other identifiers associated with the Source Well. (FIXME: check this)

Destination Well X Offset, Destination Well Y Offset
    The offsets, from the well centre, of the target position, in µm.  Positive X
    is directed toward the right of the well (positive in column 5 is closer to column 6),
    while positive Y is directed toward the top of the well (positive in row C is closer
    to row B).  Should be integers.

Transfer Volume
    The volume of liquid to be transferred, in nL.

Delay
    The time to wait before starting the transfer, in s.

Classification Type
    FIXME: unknown

Additional Kithairon Columns
----------------------------

Destination Sample Name
    The name of the destination well.

Source Concentration
    The concentration of the source sample.

Source Concentration Units
    The units of the source concentration.

well_well_index
    If specified, a specific order of transfers within a (source plate, destination plate) pair.

plate_plate_index
    If specified, a specific order of transfers between plate pairs.
