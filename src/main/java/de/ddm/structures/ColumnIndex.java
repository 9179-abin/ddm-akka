package de.ddm.structures;

import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class ColumnIndex implements AkkaSerializable {
    int fileId;
    int columnIndex;

}
