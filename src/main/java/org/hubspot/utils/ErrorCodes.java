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
    HTTP_400(145),
    HTTP_401(146),
    HTTP_403(148),
    HTTP_404(149),
    HTTP_405(150),
    HTTP_406(151),
    HTTP_407(152),
    HTTP_408(153),
    HTTP_429(174),
    DAILY_LIMIT_REACHED(174),
    HTTP_500(245),
    HTTP_501(246),
    HTTP_502(247),
    HTTP_503(248),
    HTTP_504(249);


    private final int errorCode;

    ErrorCodes(int errorCode) {
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
