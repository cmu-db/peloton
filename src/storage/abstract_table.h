/*-------------------------------------------------------------------------
*
* abstract_table.h
* the base class for all tables
*
* Copyright(c) 2015, CMU
*
* /n-store/src/storage/abstract_table.h
*
*-------------------------------------------------------------------------
*/

#pragma once

namespace nstore {

namespace storage {


class AbstractTable	{

public:

	virtual void *get(const int row, const int column);

};


}

}
