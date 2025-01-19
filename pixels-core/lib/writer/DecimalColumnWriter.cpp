/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

#include "writer/DecimalColumnWriter.h"

DecimalColumnWriter::DecimalColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption)
    : runlengthEncoding(writerOption->getUseRunLengthEncoding()), encoder(nullptr) {
    if (runlengthEncoding) {
        encoder = std::make_unique<RunLenIntEncoder>();
    }
}

int DecimalColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size) {
    auto columnVector = std::static_pointer_cast<DecimalColumnVector>(vector);
    if (!columnVector) {
        throw std::invalid_argument("Invalid vector type");
    }

    int* values = columnVector->getValues();
    int curPartLength = 0;
    int curPartOffset = 0;
    int nextPartLength = size;

    if (runlengthEncoding) {
        while ((curPixelIsNullIndex + nextPartLength) >= pixelStride) {
            curPartLength = pixelStride - curPixelIsNullIndex;
            writeCurPartDecimal(columnVector, values, curPartLength, curPartOffset);
            newPixel();
            curPartOffset += curPartLength;
            nextPartLength = size - curPartOffset;
        }
    }

    curPartLength = nextPartLength;
    writeCurPartDecimal(columnVector, values, curPartLength, curPartOffset);

    return outputStream->getWritePos();
}

bool DecimalColumnWriter::decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption) {
    return writerOption->getNullPadding();
}

void DecimalColumnWriter::writeCurPartDecimal(std::shared_ptr<DecimalColumnVector> columnVector, int* values, int curPartLength, int curPartOffset) {
    for (int i = 0; i < curPartLength; ++i) {
        int value = values[curPartOffset + i];
        if (value == NULL_DECIMAL_VALUE) {
            curPixelVector.push_back(0);
        } else {
            curPixelVector.push_back(value);
        }
    }
}

void DecimalColumnWriter::newPixel() {
    if (runlengthEncoding) {
        encoder->encode(curPixelVector);
    } else {
        for (auto value : curPixelVector) {
            outputStream->write(reinterpret_cast<char*>(&value), sizeof(value));
        }
    }
    curPixelVector.clear();
}
