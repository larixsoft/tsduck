//----------------------------------------------------------------------------
//
// TSDuck - The MPEG Transport Stream Toolkit
// Copyright (c) 2005-2026, Thierry Lelegard
// BSD-2-Clause license, see LICENSE.txt file or https://tsduck.io/license
//
//----------------------------------------------------------------------------
//!
//!  @file
//!  Base class for devices or devices which can work in non-blocking mode.
//!
//----------------------------------------------------------------------------

#pragma once
#include "tsReport.h"

namespace ts {
    //!
    //! Base class for devices or devices which can work in non-blocking mode.
    //!
    //! The methods from this class should not be used by applications. They should
    //! be used only by "reactive classes", which work in combination with a Reactor.
    //!
    //! The exact meaning of "non-blocking" depends on the type of device and the
    //! operating system. This is why this class shall be used by specialized classes
    //! which exactly know what they are doing.
    //!
    //! @ingroup libtscore reactor
    //!
    class TSCOREDLL NonBlockingDevice
    {
        TS_NOCOPY(NonBlockingDevice);
    public:
        //!
        //! Constructor.
        //!
        NonBlockingDevice() = default;

        //!
        //! Constructor.
        //! @param [in] non_blocking It true, the device is set in non-blocking mode.
        //!
        NonBlockingDevice(bool non_blocking) : _is_non_blocking(non_blocking) {}

        //!
        //! Destructor.
        //!
        virtual ~NonBlockingDevice();

        //!
        //! Set the device in non-blocking mode.
        //!
        //! Important: Usually, this method must be called before opening the device, whatever it means.
        //! Otherwise it is ignored and the device blocking mode is unchanged.
        //!
        //! @param [in] non_blocking It true, the device is set in non-blocking mode.
        //! @return True on success, false if the device is already open and the
        //! non-blocking mode is unchanged.
        //!
        bool setNonBlocking(bool non_blocking);

        //!
        //! Check if the device is in non-blocking mode.
        //! @return True if the device is in non-blocking mode, false otherwise.
        //! @see setNonBlocking()
        //!
        bool isNonBlocking() const { return _is_non_blocking; }

        //!
        //! Set a default Report for the non-blocking object.
        //!
        //! Consider a subclass of NonBlockingDevice, for instance Socket. By default, an instance is in
        //! blocking mode. All methods execute in an independent environment and these methods have a Report
        //! parameter (CERR by default). Thus, each method can report errors in an independent manner.
        //! However, when the instance is in non-blocking mode, it usually executes in the context of a
        //! Reactor and some I/O operations may execute asynchronously. In that case, the "reactive" subclass
        //! can set a default Report which will override each individual Report parameter.
        //!
        //! @param [in] report The address of a Report to use by default in all operations of the subclass.
        //! In nullptr is used, revert to the standard use of reports.
        //! @param [in] only_if_cerr If true, @a report is used only when the explicit Report is @a CERR.
        //!
        //! Example:
        //! @code
        //! obj.setNonBlocking(true);
        //! obj.setDefaultReport(some_report, true);  // note: only_if_cerr = true
        //! // ...
        //! obj.someMethod(data, size);                // use some_report (default value for Report parameter is CERR)
        //! obj.someMethod(data, size, CERR);          // use some_report
        //! obj.someMethod(data, size, other_report);  // use other_report (only_if_cerr is true and Report parameter is not CERR)
        //! @endcode
        //!
        void setDefaultReport(Report* report, bool only_if_cerr);

    protected:
        //!
        //! Check the blocking mode of a device.
        //! Called by subclass methods which are explicitly called in blocking or non-blocking mode.
        //! @param [in] non_blocking The required non-blocking mode.
        //! @param [in] opname Name of the operation, for the error message.
        //! @param [in,out] report Where to report error.
        //! @return True on success, false on error.
        //!
        bool checkNonBlocking(bool non_blocking, const UChar* opname, Report& report) const;

        //!
        //! Check the blocking mode of a device.
        //! Called by subclass methods which are explicitly called in blocking or non-blocking mode.
        //! @param [in,out] would_block Address of a boolean. If non-null, we are in non-blocking mode.
        //! When null, we are in blocking mode. When non-null, @a *would_block is reset to false.
        //! @param [in] opname Name of the operation, for the error message.
        //! @param [in,out] report Where to report error.
        //! @return True on success, false on error.
        //!
        bool checkNonBlocking(bool* would_block, const UChar* opname, Report& report) const;

        //!
        //! Check that the non-blocking mode can be set.
        //! Must be implemented by subclasses which do not support setting the non-blocking
        //! in certain states, such as after being opened. The default implementation always
        //! allows setting the non-blocking mode.
        //! @return True if setting the non-blocking mode is allowed, false otherwise.
        //!
        virtual bool allowSetNonBlocking() const;

        //!
        //! Get the actual report for an operation.
        //! @param [in,out] report The explicit Report in the operation.
        //! @return A reference to the actual report to use.
        //!
        Report& rep(Report& report) const;

    private:
        bool    _is_non_blocking = false;
        bool    _report_only_if_cerr = true;
        Report* _default_report = nullptr;
    };
}
