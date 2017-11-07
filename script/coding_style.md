This is a short document describing the preferred coding style for Peloton.

**VARIABLE AND FUNCTION NAMES**: Use descriptive variable and function names. Ex: Instead of using `i`, `j` etc. use `index_itr`, `query_counter`. GLOBAL variables (to be used only if you _really_ need them) need to have descriptive names, as do global functions.  If you have a function that counts the number of active users, you should call that "CountActiveUsers()" or similar, you should _not_ call it "Cntusr()".

**FORWARD DECLARATIONS** Use `forward declaration` in .h files as much as possible. Instead, include the required files only in the .cpp files. This will significantly reduce coupling and compilation times after minor changes.

**INDENTATION** We use 2-column tabs for indentation. Don't put multiple assignments on a single line either. Peloton coding style
is super simple. Avoid tricky expressions. Get a decent editor and don't leave whitespace at the end of lines.

**BREAKING LONG LINES** Coding style is all about readability and maintainability using commonly available tools. The limit on the length of lines is 80 columns and this is a strongly preferred limit. Statements longer than 80 columns will be broken into sensible chunks, unless exceeding 80 columns significantly increases readability and does not hide information. Descendants are always substantially shorter than the parent and are placed substantially to the right. The same applies to function headers with a long argument list. Make use of vertical spaces to separate different steps of a function.

**COMMENTS** Add comments for functions, function parameters, and logic inside the function. Please remember that code is often written once but read many times. Comments are good, but there is also a danger of over-commenting.  NEVER try to explain HOW your code works in a comment: it's much better to write the code so that the _working_ is obvious, and it's a waste of time to explain badly written code. Generally, you want your comments to tell WHAT your code does, not HOW. The style for Peloton comments is the C99-style "// ..." comments, not the C89 "/* ... */" style.

**FUNCTIONS** Functions should be short and sweet, and do just one thing.  They should fit on one or two screenfuls of text, and do one thing and do that well. The maximum length of a function is inversely proportional to the complexity and indentation level of that function.  So, if you have a conceptually simple function that is just one long (but simple) case-statement, where you have to do lots of small things for a lot of different cases, it's OK to have a longer function. However, if you have a complex function, and you suspect that a less-than-gifted first-year high-school student might not even understand what the function is all about, you should adhere to the maximum limits all the more closely.  Use helper functions with descriptive names (you can ask the compiler to in-line them if you think it's performance-critical, and it will probably do a better job of it than you would have done).

Another measure of the function is the number of local variables.  They shouldn't exceed 5-10, or you're doing something wrong.  Re-think the function, and split it into smaller pieces.  A human brain can generally easily keep track of about 7 different things, anything more and it gets confused.  You know you're brilliant, but maybe you'd like to understand what you did 2 weeks from now.

**TYPEDEFS** Please don't use things like "vps_t". It's a _mistake_ to use typedef for structures and pointers. When you see a	`vps_t a;` in the source, what does it mean? In contrast, if it says `struct virtual_container *a;` you can actually tell what "a" is.

**UNUSED CODE** Avoid commenting out unused code. Remove them.

**PRINTF** Refrain from using `printf` and `std::cout`. Instead use the logging macros, such as LOG_LEVEL_INFO, in `common/logger.h`.

**DON'T REINVENT THE MACROS** Use `PL_ASSERT` in `common/macros.h` instead of regular `assert`. This header file icontains a number of macros that you should use, rather than explicitly coding some variant of them yourself.

**PLAFORM-SPECIFIC CODE** Add platform-specific code only to `common/platform.h`.

**THE INLINE DISEASE** Don't use `inline` unless you know what you are doing. A reasonable rule of thumb is to not put inline at functions that have more than 3 lines of code in them. Abundant use of the inline keyword leads to a much bigger binary, which in turn slows the system as a whole down, due to a bigger `icache footprint` for the CPU and simply because there is less memory available for the `buffer cache`. 

**CONDITIONAL COMPILATION** Wherever possible, don't use preprocessor conditionals (#if, #ifdef) in .cpp files; doing so makes code harder to read and logic harder to follow.  Instead, use such conditionals in a header file defining functions for use in those .cpp files, providing no-op stub versions in the #else case, and then call those functions unconditionally from .cpp files.  The compiler will avoid generating any code for the stub calls, producing identical results, but the logic will remain easy to follow.

**FILE HEADERS** Add the default peloton file header if you create a new file.

**EDITOR MODELINES** Some editors can interpret configuration information embedded in source files, indicated with special markers.  For example, emacs interprets lines marked like this:	-*- mode: c -*-. Do NOT include any of these in source files. People have their own personal editor configurations, and your source files should not override them.

**ALLOCATING MEMORY** Use `PL_MEMCPY` macro in `common/macros.h`. Always use smart pointers, such as `std::unique_ptr`, to simplify memory management.

**PRINTING PELOTON MESSAGES** Peloton developers like to be seen as literate. Do mind the spelling of messages to make a good impression. Do not use crippled words like "dont"; use "do not" or "don't" instead.  Make the messages concise, clear, and unambiguous. Use appropriate log levels, such as LOG_LEVEL_TRACE, in `common/logger.h`. Coming up with good debugging messages can be quite a challenge; and once you have them, they can be a huge help for troubleshooting.

**MACROS, ENUMS** Names of macros defining constants and labels in enums are capitalized. `#define CONSTANT 0x12345`. Enums are preferred when defining several related constants. CAPITALIZED macro names are appreciated but macros resembling functions may be named in lower case. Generally, inline functions are preferable to macros resembling functions.

**DATA STRUCTURES** Add new data structures in `container` directory, so that they can be reused across the system.
