import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

const publicRoot = new URL('../public_html/', import.meta.url);
const trackingKeys = [
  'utm_source',
  'utm_medium',
  'utm_campaign',
  'utm_content',
  'utm_term',
  'utm_id',
  'ad_id',
];

function attributionScript(relativePath) {
  const source = readFileSync(new URL(relativePath, publicRoot), 'utf8');
  const marker = "const keys = ['utm_source'";
  const markerIndex = source.indexOf(marker);
  assert.notEqual(markerIndex, -1, `${relativePath} must contain the attribution key list`);

  const start = source.lastIndexOf('(function () {', markerIndex);
  const end = source.indexOf('})();', markerIndex);
  assert.notEqual(start, -1, `${relativePath} must start attribution in an IIFE`);
  assert.notEqual(end, -1, `${relativePath} must close the attribution IIFE`);
  return source.slice(start, end + '})();'.length);
}

function createSessionStorage() {
  const values = new Map();
  return {
    getItem(key) {
      return values.has(key) ? values.get(key) : null;
    },
    setItem(key, value) {
      values.set(String(key), String(value));
    },
  };
}

function runLandingAttribution({ pageUrl, referrer, links, sessionStorage }) {
  vm.runInNewContext(attributionScript('index.php'), {
    URL,
    URLSearchParams,
    document: {
      referrer,
      querySelectorAll(selector) {
        assert.equal(selector, 'a[href]');
        return links;
      },
    },
    sessionStorage,
    window: { location: new URL(pageUrl) },
  });
}

function runFormAttribution({ relativePath, pageUrl, referrer, inputs, sessionStorage }) {
  const form = {
    querySelector(selector) {
      const match = selector.match(/^\[name="([^"]+)"\]$/);
      return match ? inputs[match[1]] ?? null : null;
    },
  };

  vm.runInNewContext(attributionScript(relativePath), {
    URL,
    URLSearchParams,
    document: {
      referrer,
      querySelectorAll(selector) {
        if (selector === 'form') return [form];
        if (selector === 'a[href]') return [];
        assert.fail(`Unexpected selector: ${selector}`);
      },
    },
    sessionStorage,
    window: { location: new URL(pageUrl) },
  });
}

function inputMap() {
  return Object.fromEntries(
    [...trackingKeys, 'source_page_url', 'referrer_url'].map((key) => [key, { value: '' }]),
  );
}

test('Meta campaign parameters survive landing-page click and populate the quote form', () => {
  const campaign = {
    utm_source: 'meta',
    utm_medium: 'paid_social',
    utm_campaign: 'Industrial Scan – Québec',
    utm_content: 'Carousel A',
    utm_term: 'Manufacturing',
    utm_id: '120215001234567890',
    ad_id: '120215009876543210',
  };
  const landingUrl = new URL('https://3dpreciscan.com/');
  for (const [key, value] of Object.entries(campaign)) landingUrl.searchParams.set(key, value);

  const sessionStorage = createSessionStorage();
  const quoteLink = { href: 'https://3dpreciscan.com/soumission' };
  runLandingAttribution({
    pageUrl: landingUrl.href,
    referrer: 'https://www.facebook.com/',
    links: [quoteLink],
    sessionStorage,
  });

  const propagatedUrl = new URL(quoteLink.href);
  for (const [key, value] of Object.entries(campaign)) {
    assert.equal(propagatedUrl.searchParams.get(key), value);
  }

  const inputs = inputMap();
  runFormAttribution({
    relativePath: 'pages/soumission.php',
    pageUrl: propagatedUrl.href,
    referrer: landingUrl.href,
    inputs,
    sessionStorage,
  });

  for (const [key, value] of Object.entries(campaign)) {
    assert.equal(inputs[key].value, value);
  }
  assert.equal(inputs.source_page_url.value, landingUrl.href);
  assert.equal(inputs.referrer_url.value, 'https://www.facebook.com/');
});

test('A directly tagged Meta contact URL populates every forwarded tracking field', () => {
  const directUrl = new URL('https://3dpreciscan.com/contactez-nous');
  directUrl.searchParams.set('utm_source', 'meta');
  directUrl.searchParams.set('utm_medium', 'paid_social');
  directUrl.searchParams.set('utm_campaign', 'Direct Meta Test');
  directUrl.searchParams.set('utm_content', 'Lead creative');
  directUrl.searchParams.set('utm_term', 'Quebec manufacturers');
  directUrl.searchParams.set('utm_id', 'campaign-123');
  directUrl.searchParams.set('ad_id', 'ad-456');

  const inputs = inputMap();
  runFormAttribution({
    relativePath: 'pages/contact.php',
    pageUrl: directUrl.href,
    referrer: 'https://l.facebook.com/',
    inputs,
    sessionStorage: createSessionStorage(),
  });

  for (const key of trackingKeys) {
    assert.equal(inputs[key].value, directUrl.searchParams.get(key));
  }
  assert.equal(inputs.source_page_url.value, directUrl.href);
  assert.equal(inputs.referrer_url.value, 'https://l.facebook.com/');
});
