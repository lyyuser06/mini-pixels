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

#include "writer/TimestampColumnWriter.h"

TimestampColumnWriter::TimestampColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption)
    : runlengthEncoding(writerOption->getUseRunLengthEncoding()), encoder(nullptr) {
    if (runlengthEncoding) {
        encoder = std::make_unique<RunLenIntEncoder>();
    }
}

int TimestampColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size) {
    auto columnVector = std::static_pointer_cast<TimestampColumnVector>(vector);
    if (!columnVector) {
        throw std::invalid_argument("Invalid vector type");
    }

    long* values = columnVector->getValues();
    int curPartLength = 0;
    int curPartOffset = 0;
    int nextPartLength = size;

    if (runlengthEncoding) {
        while ((curPixelIsNullIndex + nextPartLength) >= pixelStride) {
            curPartLength = pixelStride - curPixelIsNullIndex;
            writeCurPartTimestamp(columnVector, values, curPartLength, curPartOffset);
            newPixel();
            curPartOffset += curPartLength;
            nextPartLength = size - curPartOffset;
        }
    }

    curPartLength = nextPartLength;
    writeCurPartTimestamp(columnVector, values, curPartLength, curPartOffset);

    return outputStream->getWritePos();
}

bool TimestampColumnWriter::decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption) {
    return writerOption->getNullPadding();
}

void TimestampColumnWriter::writeCurPartTimestamp(std::shared_ptr<TimestampColumnVector> columnVector, long* values, int curPartLength, int curPartOffset) {
    for (int i = 0; i < curPartLength; ++i) {
        long value = values[curPartOffset + i];
        if (value == NULL_TIMESTAMP_VALUE) {
            curPixelVector.push_back(0);
        } else {
            curPixelVector.push_back(value);
        }
    }
}

void TimestampColumnWriter::newPixel() {
    if (runlengthEncoding) {
        encoder->encode(curPixelVector);
    } else {
        for (auto value : curPixelVector) {
            outputStream->write(reinterpret_cast<char*>(&value), sizeof(value));
        }
    }
    curPixelVector.clear();
}
