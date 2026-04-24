# ChatGPT with a simple interface

All apps I have ever used jump-scroll eratically while a message streams in, making it impossible to read while streaming (and if so, why stream at all?). This single-page HTML app doesn't do any special scrolling: text streams in below the bottom of the screen, leaving the scrolling entirely in the control of the user.

Also, this app lets the user change the history of the conversation (by re-asking an old question), which can be used to cut unproductive digressions out of ChatGPT's context window.

History is never saved; reloading the page starts from scratch, which better fits how I use it. It also doesn't handle any API except OpenAI. I wrote it for my own use.

This app also saves your OpenAI API key in `localStorage`, so make a new API key exclusively for this page and monitor its use. If any Javascript gets access to this page, it can get your key and charge your account. This page has zero Javascript dependencies to prevent that kind of security breach, but if you modify the code, be careful!

Moved to jpivarski-secure.github.io so that no other repos can see this one's `localStorage` or `IndexedDB`.
