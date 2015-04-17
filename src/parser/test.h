#pragma once

#include <vector>
#include <string>
#include <iostream>

namespace nstore {
namespace parser {

#define TESTX(name) \
	void name(); \
	namespace g_dummy { int _##name = AddTest(name, #name); } \
	void name()

class AssertionFailedException: public std::exception {
public:
	AssertionFailedException(std::string msg) :
		std::exception(),
		_msg(msg) {};

	virtual const char* what() const throw() {
		return _msg.c_str();
	}

protected:
	std::string _msg;
};

int AddTest(void (*foo)(void), std::string name);

void RunTests();

} // End parser namespace
} // End nstore namespace
