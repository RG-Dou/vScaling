package samzaapps.ad;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A page view event
 */
public class PageView {
    public final String userId;
    public final String country;
    public final String pageId;

    /**
     * Constructs a page view event.
     *
     * @param pageId the id for the page that was viewed
     * @param userId the user that viewed the page
     * @param country the country that the page was viewed from
     */
    public PageView(
            @JsonProperty("pageId") String pageId,
            @JsonProperty("userId") String userId,
            @JsonProperty("countryId") String country) {
        this.userId = userId;
        this.country = country;
        this.pageId = pageId;
    }
}