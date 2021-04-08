package org.hubspot.objects.crm.engagements;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * @author Nicholas Curl
 */
public class Email extends Engagement {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();
    private final String body;
    private final List<Details> to;
    private final List<Details> cc;
    private final List<Details> bcc;
    private final Details from;
    private final String subject;

    public Email(long id,
                 List<Details> to,
                 List<Details> cc,
                 List<Details> bcc,
                 Details from,
                 String subject,
                 String body
    ) {
        super(id);
        this.to = to;
        this.cc = cc;
        this.bcc = bcc;
        this.from = from;
        this.subject = subject;
        this.body = body;
    }

    public List<Details> getBcc() {
        return bcc;
    }

    public String getBody() {
        return body;
    }

    public List<Details> getCc() {
        return cc;
    }

    public Details getFrom() {
        return from;
    }

    public String getSubject() {
        return subject;
    }

    public List<Details> getTo() {
        return to;
    }

    @Override
    public String toString() {
        StringBuilder toBuilder = new StringBuilder();
        StringBuilder ccBuilder = new StringBuilder();
        StringBuilder bccBuilder = new StringBuilder();
        for (int i = 0; i < to.size(); i++) {
            if ((i + 1) == to.size()) {
                toBuilder.append(to.get(i));
            } else {
                toBuilder.append(to.get(i)).append(", ");
            }
        }
        for (int i = 0; i < cc.size(); i++) {
            if ((i + 1) == cc.size()) {
                ccBuilder.append(cc.get(i));
            } else {
                ccBuilder.append(cc.get(i)).append(", \n\t");
            }
        }
        for (int i = 0; i < bcc.size(); i++) {
            if ((i + 1) == bcc.size()) {
                bccBuilder.append(bcc.get(i));
            } else {
                bccBuilder.append(bcc.get(i)).append(", ");
            }
        }
        return "To:\t" + toBuilder.toString() +
                "\nCC:\t" + ccBuilder.toString() +
                "\nBCC:\t" + bccBuilder.toString() +
                "\nFrom:\t" + from +
                "\nSubject:\t" + subject +
                "\n" + body;
    }

    public static class Details {
        private final String firstName;
        private final String lastName;
        private final String email;

        public Details(String firstName, String lastName, String email) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.email = email;
        }

        public String getEmail() {
            return email;
        }

        public String getFirstName() {
            return firstName;
        }

        public String getLastName() {
            return lastName;
        }

        @Override
        public String toString() {
            return lastName + ", " + firstName + " <" + email + ">";
        }
    }
}
