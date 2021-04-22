package org.hubspot.objects.crm.engagements;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * A class that represents an email engagement
 *
 * @author Nicholas Curl
 */
public class Email extends Engagement {

    /**
     * The instance of the logger
     */
    private static final Logger        logger           = LogManager.getLogger();
    /**
     * The serial version UID for this class
     */
    private static final long          serialVersionUID = -2353192631944889158L;
    /**
     * The body of the email
     */
    private final        String        body;
    /**
     * The list of email addresses that the email is being sent to
     */
    private final        List<Details> to;
    /**
     * The list of email addresses that are being Cc'd
     */
    private final        List<Details> cc;
    /**
     * The list of email addresses that are being BCc'd
     */
    private final        List<Details> bcc;
    /**
     * The email address sending the email
     */
    private final        Details       from;
    /**
     * The email's subject line
     */
    private final        String        subject;

    /**
     * A constructor for an Email object
     *
     * @param id      The engagement id
     * @param to      The list of email addresses that this email is being sent to
     * @param cc      The list of email addresses that are being Cc'd
     * @param bcc     The list of email addresses that are being BCc'd
     * @param from    The email address sending the email
     * @param subject The subject line of the email
     * @param body    The body of the email
     */
    public Email(final long id,
                 final List<Details> to,
                 final List<Details> cc,
                 final List<Details> bcc,
                 final Details from,
                 final String subject,
                 final String body
    ) {
        super(id);
        this.to = to;
        this.cc = cc;
        this.bcc = bcc;
        this.from = from;
        this.subject = subject;
        this.body = body;
    }

    /**
     * Get the list of email address that are being BCc'd
     *
     * @return The list of email addresses that are being BCc'd
     */
    public List<Details> getBcc() {
        return bcc;
    }

    /**
     * Gets the body of the email
     *
     * @return The body of the email
     */
    public String getBody() {
        return body;
    }

    /**
     * Gets the list of email addresses that are being Cc'd
     *
     * @return The list of email addresses that are being Cc'd
     */
    public List<Details> getCc() {
        return cc;
    }

    /**
     * Gets the email address sending this email
     *
     * @return The email address sending this email
     */
    public Details getFrom() {
        return from;
    }

    /**
     * Gets the subject line of the email
     *
     * @return The subject line of the email
     */
    public String getSubject() {
        return subject;
    }

    /**
     * Gets the list of email addresses that are being sent this email
     *
     * @return The list of email addresses that are being sent this email
     */
    public List<Details> getTo() {
        return to;
    }

    /**
     * Returns the string representation of the email
     *
     * @return The string representation of the email
     */
    @Override
    public String toString() {
        final StringBuilder toBuilder = new StringBuilder();
        final StringBuilder ccBuilder = new StringBuilder();
        final StringBuilder bccBuilder = new StringBuilder();
        for (int i = 0; i < to.size(); i++) {
            if ((i + 1) == to.size()) {
                toBuilder.append(to.get(i));
            }
            else {
                toBuilder.append(to.get(i)).append(", ");
            }
        }
        for (int i = 0; i < cc.size(); i++) {
            if ((i + 1) == cc.size()) {
                ccBuilder.append(cc.get(i));
            }
            else {
                ccBuilder.append(cc.get(i)).append(", \n\t");
            }
        }
        for (int i = 0; i < bcc.size(); i++) {
            if ((i + 1) == bcc.size()) {
                bccBuilder.append(bcc.get(i));
            }
            else {
                bccBuilder.append(bcc.get(i)).append(", ");
            }
        }
        return "To:\t" +
               toBuilder +
               "\nCC:\t" +
               ccBuilder +
               "\nBCC:\t" +
               bccBuilder +
               "\nFrom:\t" +
               from +
               "\nSubject:\t" +
               subject +
               "\n" +
               body;
    }

    /**
     * An inner-class that holds the information regarding email addresses
     */
    public static class Details {

        /**
         * The first name associated to the email address
         */
        private final String firstName;
        /**
         * The last name associated to the email address
         */
        private final String lastName;
        /**
         * The email address
         */
        private final String emailAddress;

        /**
         * A constructor for an email address details
         *
         * @param firstName    The first name associated to the email address
         * @param lastName     The last name associated to the email address
         * @param emailAddress The email address
         */
        public Details(final String firstName, final String lastName, final String emailAddress) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.emailAddress = emailAddress;
        }

        /**
         * Gets the email address
         *
         * @return The email address
         */
        public String getEmailAddress() {
            return emailAddress;
        }

        /**
         * Gets the first name associated to the email address
         *
         * @return The first name associated to the email address
         */
        public String getFirstName() {
            return firstName;
        }

        /**
         * Gets the last name associated to the email address
         *
         * @return The last name associated to the email address
         */
        public String getLastName() {
            return lastName;
        }

        /**
         * Returns the string representation of this object
         *
         * @return The string representation of this object
         */
        @Override
        public String toString() {
            return lastName + ", " + firstName + " <" + emailAddress + ">";
        }
    }
}
