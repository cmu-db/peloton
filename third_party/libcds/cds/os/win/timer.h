/*
    This file is a part of libcds - Concurrent Data Structures library

    (C) Copyright Maxim Khizhinsky (libcds.dev@gmail.com) 2006-2016

    Source code repo: http://github.com/khizmax/libcds/
    Download: http://sourceforge.net/projects/libcds/files/
    
    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice, this
      list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
    AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
    IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
    DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
    FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
    DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
    SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
    OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
    OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.     
*/

#ifndef CDSLIB_OS_WIN_TIMER_H
#define CDSLIB_OS_WIN_TIMER_H

#ifndef CDSLIB_OS_TIMER_H
#   error "<cds/os/timer.h> must be included"
#endif

#ifndef NOMINMAX
#   define NOMINMAX
#endif
#include <windows.h>

//@cond none
namespace cds { namespace OS {
    CDS_CXX11_INLINE_NAMESPACE namespace Win32 {

        /// High resolution timer
        /**
            Implementation of high resolution timer for Windows platforms.
            The implementation build on QueryPerformanceCounter API.
        */
        class Timer {
        public:
            typedef LARGE_INTEGER    native_timer_type        ;    ///< Native timer type
            typedef long long        native_duration_type    ;    ///< Native duration type

        private:
            native_timer_type    m_nFrequency;
            native_timer_type    m_nStart;

        public:
            Timer()
            {
                ::QueryPerformanceFrequency( &m_nFrequency );
                current( m_nStart );
            }

            /// Places into \p tmr the current time in native Windows format
            static void current( native_timer_type& tmr )
            {
                ::QueryPerformanceCounter( &tmr );
            }

            /// Returns current time in native Windows format
            static native_timer_type current()
            {
                native_timer_type    tmr;
                current(tmr);
                return tmr;
            }

            /// Sets internal start time to current time. Returns duration from prevoius start time to current.
            double reset()
            {
                native_timer_type nCur;
                current( nCur );
                double dblRet = double(nCur.QuadPart - m_nStart.QuadPart) / m_nFrequency.QuadPart;
                m_nStart.QuadPart = nCur.QuadPart;
                return dblRet;
            }

            /// Translates \p dur from native format to seconds
            double duration( native_duration_type dur )
            {
                return double( dur ) / m_nFrequency.QuadPart;
            }

            /// Returns duration (in seconds) from start time to current
            double duration()
            {
                return duration( native_duration() );
            }

            /// Returns duration (in native format) from start time to current
            native_duration_type    native_duration()
            {
                native_timer_type ts;
                current( ts );
                return native_duration( m_nStart, ts );
            }

            /// Calculates duration (in native format) between \p nEnd and \p nStart
            static native_duration_type    native_duration( const native_timer_type& nStart, const native_timer_type& nEnd )
            {
                return nEnd.QuadPart - nStart.QuadPart;
            }

            static unsigned long long random_seed()
            {
                return current().QuadPart;
            }
        };


    }   // namespace Win32

#ifndef CDS_CXX11_INLINE_NAMESPACE_SUPPORT
    typedef Win32::Timer    Timer;
#endif

}}  // namespae cds::OS
//@endcond

#endif // #ifndef CDSLIB_OS_WIN_TIMER_H
