package org.hubspot.utils;

/**
 * @author Nicholas Curl
 */
public enum ErrorCodes {
    GENERAL(1),
    HUBSPOT_EXCEPTION(131),
    IO_EXCEPTION(132),
    IO_WRITE(133),
    IO_READ(134),
    IO_CREATE_DIRECTORY(135),
    IO_DELETE_DIRECTORY(136),
    NULL_EXCEPTION(137),
    UNIREST_EXCEPTION(138),
    THREAD_INTERRUPT_EXCEPTION(139),
    INVALID_ENGAGEMENT(140),
    NUMBER_FORMAT_EXCEPTION(141),
    HTTP_400(144),
    HTTP_401(145),
    HTTP_403(147),
    HTTP_404(148),
    HTTP_405(149),
    HTTP_406(150),
    HTTP_407(151),
    HTTP_408(152),
    HTTP_409(153),
    HTTP_429(173),
    DAILY_LIMIT_REACHED(173),
    HTTP_500(244),
    HTTP_501(245),
    HTTP_502(246),
    HTTP_503(247),
    HTTP_504(248),
    HTTP_524(12);
    private final int errorCode;

    ErrorCodes(int errorCode) {
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
