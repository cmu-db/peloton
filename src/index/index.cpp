/*-------------------------------------------------------------------------
 *
 * index.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/index/index.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <iostream>

#include "index/index.h"

namespace nstore {
namespace index {

TableIndex::TableIndex(const TableIndexScheme &scheme) : m_scheme(scheme)
{
    name_ = scheme.name;
    column_indices_vector_ = scheme.columnIndices;
    column_types_vector_ = scheme.columnTypes;
    colCount_ = (int)column_indices_vector_.size();
    is_unique_index_ = scheme.unique;
    m_tupleSchema = scheme.tupleSchema;
    assert(column_types_vector_.size() == column_indices_vector_.size());
    column_indices_ = new int[colCount_];
    column_types_ = new ValueType[colCount_];
    for (int i = 0; i < colCount_; ++i)
    {
        column_indices_[i] = column_indices_vector_[i];
        column_types_[i] = column_types_vector_[i];
    }
    m_keySchema = scheme.keySchema;
    // initialize all the counters to zero
    m_lookups = m_inserts = m_deletes = m_updates = 0;
}

TableIndex::~TableIndex(){
    delete[] column_indices_;
    delete[] column_types_;

    delete m_keySchema;
}

std::string TableIndex::debug() const
{
    std::ostringstream buffer;
    buffer << this->getTypeName() << "(" << this->getName() << ")";
    buffer << (isUniqueIndex() ? " UNIQUE " : " NON-UNIQUE ");
    //
    // Columns
    //
    buffer << " -> Columns[";
    std::string add = "";
    for (int ctr = 0; ctr < this->colCount_; ctr++) {
        buffer << add << ctr << "th entry=" << this->column_indices_[ctr]
               << "th (" << ValueToString(column_types_[ctr])
               << ") column in parent table";
        add = ", ";
    }
    buffer << "] --- size: " << this->getSize();

    std::string ret(buffer.str());
    return (ret);
}

void TableIndex::printReport()
{
    std::cout << name_ << ",";
    std::cout << getTypeName() << ",";
    std::cout << m_lookups << ",";
    std::cout << m_inserts << ",";
    std::cout << m_deletes << ",";
    std::cout << m_updates << std::endl;
}

bool TableIndex::equals(const TableIndex *other) const{
    //TODO Do something useful here!
    return true;
}

} // End index namespace
} // End nstore namespace


