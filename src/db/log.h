/**********************************************
   File:   log.h

   This code is based on the sources of a log class published in Dr.Dobbs

   Copyright 2012 Michael Popov

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 **********************************************/

#ifndef GCOUNT_UTILS_LOG_H
#define GCOUNT_UTILS_LOG_H

#include <sstream>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>

enum LogLevel {LL_CRITICAL, LL_ERROR, LL_WARN, LL_INFO, LL_TRACE, LL_DEBUG};

template <typename T>
class Log
{
public:
    Log();
    virtual ~Log();
    std::ostringstream& Get( LogLevel level, const char* file, int line );
public:
    static LogLevel& Level();
    static const char* ToString(LogLevel level);
    static LogLevel FromString(const char* level);
protected:
    std::ostringstream os;
private:
    Log(const Log&);
    Log& operator =(const Log&);
};

template <typename T>
Log<T>::Log()
{
}

template <typename T>
std::ostringstream& Log<T>::Get(LogLevel level, const char* file, int line )
{
    char header[256]  = { '\0' };
    char str_time[64] = { '\0' };
    char str_date[64] = { '\0' };

    time_t t;
    time(&t);
    tm r;
    localtime_r(&t, &r);
    strftime(str_time, sizeof(str_time), "%X", localtime_r(&t, &r));
    strftime(str_date, sizeof(str_date), "%F", localtime_r(&t, &r));
    //snprintf(header, sizeof(header), "%d  %s  %s  ", getpid(), str_date, str_time);
    snprintf(header, sizeof(header), "%d %s %s ", getpid(), str_date, str_time);

    os << header << ToString(level);
    if (file) os << file << ":" << line << "\t";

    return os;
}

template <typename T>
Log<T>::~Log()
{
    os << std::endl;
    T::Output(os.str());
}

template <typename T>
LogLevel& Log<T>::Level()
{
    static LogLevel level = LL_INFO;
    return level;
}

template <typename T>
const char* Log<T>::ToString(LogLevel level)
{
    static const char* const buffer[] = {
        "CRITICAL ",
        "ERROR    ", 
        "WARN     ", 
        "INFO     ", 
        "TRACE    ", 
        "DEBUG    " 
    };
    return ((size_t)level < sizeof(buffer)/sizeof(*buffer)) ? buffer[level] : "UNKNOWN";
}

template <typename T>
LogLevel Log<T>::FromString(const char* level)
{
    if (strcasecmp(level, "DEBUG") == 0) return LL_DEBUG;
    if (strcasecmp(level, "TRACE") == 0) return LL_TRACE;
    if (strcasecmp(level, "INFO") == 0) return LL_INFO;
    if (strcasecmp(level, "WARN") == 0) return LL_WARN;
    if (strcasecmp(level, "ERROR") == 0) return LL_ERROR;
    if (strcasecmp(level, "CRITICAL") == 0) return LL_CRITICAL;

    return LL_INFO;
}

class OutputWriter
{
public:
    static void Output(const std::string& msg) {
        ssize_t ret = write(2, msg.c_str(), msg.length());
        (void)ret;
    }
};

class FILELog : public Log<OutputWriter> {};

#define FILE_LOG(level,file,line) \
    if (level > FILELog::Level()) ; \
    else FILELog().Get(level,file,line)

#define LOG_CRITICAL  FILE_LOG(LL_CRITICAL,__FILE__,__LINE__)
#define LOG_ERROR     FILE_LOG(LL_ERROR,NULL,0)
#define LOG_WARN      FILE_LOG(LL_WARN,NULL,0)
#define LOG_INFO      FILE_LOG(LL_INFO,NULL,0)
#define LOG_TRACE     FILE_LOG(LL_TRACE,NULL,0)
#define LOG_DEBUG     FILE_LOG(LL_DEBUG,__FILE__,__LINE__)

#define CRITICAL_EXIT(x) { LOG_CRITICAL << x; exit(EXIT_FAILURE); }

class TempLogLevel
{
public:
    TempLogLevel(LogLevel newLevel) { m_old = FILELog::Level(); FILELog::Level() = newLevel; }
    TempLogLevel(const char* level) { m_old = FILELog::Level(); FILELog::Level() = FILELog::FromString(level); }
    TempLogLevel(const std::string& level) { m_old = FILELog::Level(); FILELog::Level() = FILELog::FromString(level.c_str()); }
    ~TempLogLevel() { FILELog::Level() = m_old; }
private:
    LogLevel m_old;
};

inline void set_log_level(LogLevel log_level) {
    FILELog::Level() = log_level;
}
#endif //GCOUNT_UTILS_LOG_H
